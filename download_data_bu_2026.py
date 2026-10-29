"""
Baixa, descompacta e converte para Parquet os boletins de urna do TSE
(eleições 2026), por turno e por UF. Os dois turnos ficam na mesma página:

  https://dadosabertos.tse.jus.br/dataset/resultados-2026-boletim-de-urna

O turno de cada arquivo é identificado pelo nome (bweb_1t_<UF>_..., bweb_2t_<UF>_...),
então os arquivos do 2º turno são pegos automaticamente assim que o TSE os
publicar nessa página; é só rodar o script de novo.

Pastas criadas dentro de --base (padrão: pasta atual):

  boletins_urna_1turno            .zip baixados
  boletins_urna_1turno_csv        CSVs extraídos (uma subpasta por .zip)
  boletins_urna_1turno_parquet    um .parquet por .zip
  (e o mesmo para boletins_urna_2turno*)

Como funciona
- Os links são descobertos pela API do CKAN do portal (os mesmos do botão
  "Ir para recurso"); se a API falhar, usa o HTML da página. Só entram os
  arquivos .zip: os de hash (.sha512) são ignorados.
- Download com retomada (.part); arquivos já baixados/extraídos/convertidos
  são pulados, então é seguro interromper (Ctrl+C) e rodar de novo.
- A conversão lê o CSV em blocos (streaming) e grava o Parquet aos poucos,
  um arquivo por vez, então a RAM usada não depende do tamanho do CSV.
  Todas as colunas são gravadas como texto (ver --help / comentário abaixo).
- O Parquet guarda só as linhas com DS_CARGO_PERGUNTA = Presidente (igualdade
  exata, sem diferenciar maiúsculas/minúsculas). Mude com --filtro COLUNA=VALOR
  ou desligue com --sem-filtro. O CSV extraído continua completo.

Requisitos: Python 3.8+. Download e descompactação usam só a biblioteca padrão;
para o Parquet:  pip install pyarrow

Exemplos
    python baixar_boletins_urna.py --listar
    python baixar_boletins_urna.py --turno 1 --ufs AC          # teste pequeno
    python baixar_boletins_urna.py --turno 2                   # só o 2º turno
    python baixar_boletins_urna.py --turno 2 --listar          # confere os links do 2º turno
    python baixar_boletins_urna.py                             # tudo
    python baixar_boletins_urna.py --apagar-zip --apagar-csv   # economiza disco
    python baixar_boletins_urna.py --etapas baixar             # só baixa
    python baixar_boletins_urna.py --reconverter               # refaz os .parquet
"""
from __future__ import annotations

import argparse
import codecs
import json
import re
import shutil
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from http.client import HTTPException
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from urllib.parse import quote, urlparse, unquote

BASE_URL = "https://dadosabertos.tse.jus.br"

DATASET_ID = "resultados-2026-boletim-de-urna"  # 1º e 2º turnos na mesma página
TURNOS = (1, 2)
ETAPAS = ("baixar", "extrair", "parquet")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
}
CHUNK = 256 * 1024
TIMEOUT = 60

# Ex.: bweb_1t_AC_051020261403.zip -> turno=1, uf=AC, carimbo=ddmmaaaaHHMM
# (tolerante: carimbo com qualquer nº de dígitos e sufixos antes do .zip)
NOME_RE = re.compile(
    r"bweb_(?P<turno>\d)t_(?P<uf>[A-Z]{2})_(?P<carimbo>\d+)[^/]*\.zip$", re.I)
# Turno citado no nome/descrição do recurso no portal: "2º turno", "2o turno", "turno 2"
TURNO_TEXTO_RE = re.compile(
    r"(?:\b([12])\s*[º°ªo.]?\s*turno|turno\s*([12])\b|\b(primeiro|segundo)\s+turno)", re.I)
# Links das páginas de recurso dentro da página do dataset
RECURSO_RE = re.compile(r'href="(/dataset/[^"/]+/resource/[0-9a-f-]{36})"', re.I)

GB = 1024 ** 3
MARGEM_DISCO = 2 * GB        # sobra mínima de disco que o script exige
FATOR_PARQUET = 0.3          # estimativa conservadora (sem filtro): parquet+zstd << CSV
FATOR_PARQUET_FILTRADO = 0.1  # idem, com o filtro de cargo ligado
FILTRO_PADRAO = "DS_CARGO_PERGUNTA=Presidente" #Filtro aplicado para criação dos parquet
BLOCO_CSV = 32 * 1024 * 1024  # bytes de CSV lidos por bloco (controla a RAM)


@dataclass
class Pastas:
    zip: Path
    csv: Path
    parquet: Path

    @classmethod
    def de(cls, base: Path, turno: int) -> "Pastas":
        nome = f"boletins_urna_{turno}turno"
        return cls(base / nome, base / f"{nome}_csv", base / f"{nome}_parquet")


# --------------------------------------------------------------------------
# Descoberta dos links
# --------------------------------------------------------------------------
def http_get(url: str, sem_cache: bool = True) -> bytes:
    if sem_cache:  # evita receber uma lista antiga guardada em cache/CDN
        url += ("&" if "?" in url else "?") + f"_={int(time.time())}"
    headers = dict(HEADERS, **{"Cache-Control": "no-cache", "Pragma": "no-cache"})
    with urlopen(Request(url, headers=headers), timeout=TIMEOUT) as resp:
        return resp.read()


def _eh_zip_dados(url: str) -> bool:
    """Aceita somente arquivos ZIP de dados; ignora recursos Hash SHA2 (.sha512)."""
    caminho = unquote(urlparse(url).path).lower()
    nome = caminho.rsplit("/", 1)[-1]
    return nome.endswith(".zip") and not nome.endswith((".sha512.zip", ".sha256.zip"))


def extrair_zips_do_html(texto: str) -> set[str]:
    """Extrai URLs .zip presentes no HTML (qualquer host do TSE)."""
    links = re.findall(r"""https?://[\w.-]*tse\.jus\.br/[^\s"'<>)\]]+""", texto, flags=re.I)
    limpos = {u.rstrip(".,;").replace("&amp;", "&") for u in links}
    return {u for u in limpos if _eh_zip_dados(u)}


def _turno_do_texto(*textos: str) -> int | None:
    for t in textos:
        m = TURNO_TEXTO_RE.search(t or "")
        if m:
            if m[3]:
                return 1 if m[3].lower() == "primeiro" else 2
            return int(m[1] or m[2])
    return None


def listar_recursos(dataset_id: str) -> dict[str, int | None]:
    """{url: turno indicado no nome/descrição do recurso (ou None)}.
    Une API CKAN e HTML da página para não perder recursos recém-publicados."""
    encontrados: dict[str, int | None] = {}
    api = f"{BASE_URL}/api/3/action/package_show?id={dataset_id}"
    try:
        dados = json.loads(http_get(api))
        for r in dados.get("result", {}).get("resources", []):
            url = (r.get("url") or "").strip()
            if _eh_zip_dados(url):
                encontrados[url] = _turno_do_texto(r.get("name"), r.get("description"))
    except (OSError, ValueError, KeyError, TypeError) as e:
        print(f"[aviso] API CKAN indisponível para {dataset_id} ({e}).", file=sys.stderr)

    try:
        html = http_get(f"{BASE_URL}/dataset/{dataset_id}").decode("utf-8", errors="replace")
        for url in extrair_zips_do_html(html):
            encontrados.setdefault(url, None)
        # Cada recurso tem uma página própria (/resource/<id>) com o link "Ir para
        # recurso". Se a API e a página do dataset não trouxeram todos, lê essas páginas.
        paginas = sorted(set(RECURSO_RE.findall(html)))
        if len(paginas) > len(encontrados):
            print(f"  lendo {len(paginas)} páginas de recurso de {dataset_id}...", flush=True)
            with ThreadPoolExecutor(max_workers=8) as ex:
                for links in ex.map(_zips_da_pagina_recurso, paginas):
                    for url, turno in links.items():
                        if encontrados.get(url) is None:
                            encontrados[url] = turno
    except OSError as e:
        print(f"[aviso] não foi possível ler o HTML de {dataset_id} ({e}).", file=sys.stderr)
    return encontrados


def _zips_da_pagina_recurso(caminho: str) -> dict[str, int | None]:
    try:
        html = http_get(BASE_URL + caminho).decode("utf-8", errors="replace")
    except OSError:
        return {}
    titulo = re.search(r"<title>(.*?)</title>", html, re.S | re.I)
    turno = _turno_do_texto(titulo[1] if titulo else "")
    return {u: turno for u in extrair_zips_do_html(html)}


def listar_urls(datasets: list[str]) -> dict[str, int | None]:
    """Recursos de todos os datasets, sem repetir arquivos com o mesmo nome."""
    unicos: dict[str, tuple[str, int | None]] = {}
    for ds in datasets:
        for url, turno in listar_recursos(ds).items():
            nome = _nome(url)
            if nome not in unicos or (unicos[nome][1] is None and turno is not None):
                unicos[nome] = (url, turno)
    if not unicos:
        raise OSError("nenhum arquivo .zip encontrado (API e HTML)")
    return {url: turno for url, turno in sorted(unicos.values())}


def _nome(url: str) -> str:
    """Nome do arquivo sem query string (?...) nem fragmento."""
    return unquote(urlparse(url).path).rsplit("/", 1)[-1]


def _stem(url: str) -> str:
    return Path(_nome(url)).stem


def _chave_versao(carimbo: str) -> str:
    """ddmmaaaaHHMM -> aaaammddHHMM (ordenável)."""
    if len(carimbo) != 12:
        return carimbo
    return carimbo[4:8] + carimbo[2:4] + carimbo[0:2] + carimbo[8:12]


def agrupar_por_turno(recursos: dict[str, int | None]
                      ) -> tuple[dict[int, list[str]], list[str]]:
    """Separa os links por turno: primeiro pelo nome do arquivo (bweb_2t_...),
    depois pelo nome/descrição do recurso no portal ("2º turno").
    Retorna (por_turno, fora_do_padrao)."""
    por_turno: dict[int, list[str]] = {}
    fora_do_padrao = []
    for u, turno_texto in recursos.items():
        m = NOME_RE.search(_nome(u))
        turno = int(m["turno"]) if m else (turno_texto or _turno_do_texto(_nome(u)))
        if turno:
            por_turno.setdefault(turno, []).append(u)
        else:
            fora_do_padrao.append(u)
    return por_turno, fora_do_padrao


def filtrar(urls: list[str], ufs: set[str] | None = None,
            so_mais_recente: bool = False) -> list[str]:
    sel = []
    for u in urls:
        m = NOME_RE.search(_nome(u))
        if ufs and not (m and m["uf"].upper() in ufs):
            continue
        sel.append((u, m))

    if so_mais_recente:  # uma UF pode ter várias versões (ex.: 24/10 e 25/10)
        ultimo: dict[str, tuple[str, str]] = {}
        for u, m in sel:
            if m:
                uf, chave = m["uf"].upper(), _chave_versao(m["carimbo"])
                if uf not in ultimo or chave > ultimo[uf][0]:
                    ultimo[uf] = (chave, u)
        manter = {u for _, u in ultimo.values()}
        sel = [(u, m) for u, m in sel if not m or u in manter]
    return [u for u, _ in sel]


# --------------------------------------------------------------------------
# Etapa 1 - download
# --------------------------------------------------------------------------
def _mb(n: int) -> str:
    return f"{n / 1024 / 1024:,.1f} MB"


def _gb(n: float) -> str:
    return f"{n / GB:,.1f} GB"


def _n(x: int) -> str:
    return f"{x:,}".replace(",", ".")


def _dur(seg: float) -> str:
    return f"{int(seg // 60)}m{int(seg % 60):02d}s"


def _baixar_uma_vez(url: str, destino: Path, progresso: bool) -> int:
    nome = _nome(url)
    final = destino / nome
    parcial = destino / (nome + ".part")
    inicio = parcial.stat().st_size if parcial.exists() else 0

    headers = dict(HEADERS)
    if inicio:
        headers["Range"] = f"bytes={inicio}-"

    try:
        resp = urlopen(Request(url, headers=headers), timeout=TIMEOUT)
    except HTTPError as e:
        if e.code == 416:  # Range fora do arquivo: .part completo ou inválido
            total = (e.headers.get("Content-Range") or "").rpartition("/")[2]
            if total.isdigit() and int(total) == inicio:
                parcial.replace(final)
                return inicio
            parcial.unlink(missing_ok=True)  # recomeça do zero na próxima tentativa
        raise

    with resp:
        if inicio and resp.status != 206:  # servidor ignorou o Range
            inicio = 0
        restante = int(resp.headers.get("Content-Length") or 0)
        total = inicio + restante if restante else 0
        baixado = inicio
        ultimo_print = 0.0

        with open(parcial, "ab" if inicio else "wb") as f:
            while True:
                bloco = resp.read(CHUNK)
                if not bloco:
                    break
                f.write(bloco)
                baixado += len(bloco)
                agora = time.monotonic()
                if progresso and agora - ultimo_print >= 0.5:
                    ultimo_print = agora
                    pct = f" {baixado / total:6.1%}" if total else ""
                    print(f"\r  {nome}: {_mb(baixado)}{pct}   ", end="", flush=True)

    if progresso:
        print("\r" + " " * 70 + "\r", end="")
    if total and baixado != total:
        raise OSError(f"download incompleto ({baixado} de {total} bytes)")

    parcial.replace(final)
    return baixado


def baixar(url: str, destino: Path, tentativas: int, progresso: bool) -> tuple[bool, str]:
    nome = _nome(url)
    if (destino / nome).exists():
        return True, f"[pulado] {nome} (já existe)"

    ultimo_erro: Exception | None = None
    for n in range(1, tentativas + 1):
        try:
            tamanho = _baixar_uma_vez(url, destino, progresso)
            return True, f"[ok]     {nome} ({_mb(tamanho)})"
        except (OSError, HTTPException) as e:
            ultimo_erro = e
            if isinstance(e, HTTPError) and e.code in (401, 403, 404, 410):
                break  # não adianta tentar de novo
            if n < tentativas:
                espera = min(2 ** n, 30)
                print(f"\n  {nome}: tentativa {n}/{tentativas} falhou ({e}); "
                      f"nova tentativa em {espera}s...", file=sys.stderr)
                time.sleep(espera)
    return False, f"[ERRO]   {nome}: {ultimo_erro}"


# --------------------------------------------------------------------------
# Etapa 2 - descompactar
# --------------------------------------------------------------------------
def garantir_espaco(pasta: Path, necessario: int, o_que: str) -> None:
    livre = shutil.disk_usage(pasta).free
    if livre < necessario + MARGEM_DISCO:
        raise OSError(
            f"espaço em disco insuficiente para {o_que}: livre {_gb(livre)}, "
            f"necessário ~{_gb(necessario + MARGEM_DISCO)}. Libere espaço ou use "
            f"--apagar-zip / --apagar-csv.")


def listar_csvs(pasta: Path) -> list[Path]:
    return sorted(p for p in pasta.rglob("*")
                  if p.is_file() and p.suffix.lower() == ".csv")


def extrair(zip_path: Path, pasta_csv: Path, fator_disco: float) -> Path:
    """Extrai para pasta_csv/<nome do zip>/ (atômico: .tmp -> nome final)."""
    alvo = pasta_csv / zip_path.stem
    if alvo.is_dir():
        return alvo
    pasta_csv.mkdir(parents=True, exist_ok=True)
    tmp = pasta_csv / (zip_path.stem + ".tmp")
    shutil.rmtree(tmp, ignore_errors=True)

    with zipfile.ZipFile(zip_path) as zf:
        total = sum(i.file_size for i in zf.infolist())
        garantir_espaco(pasta_csv, int(total * fator_disco), f"extrair {zip_path.name}")
        print(f"  extraindo {zip_path.name} ({_gb(total)} descompactado)...", flush=True)
        try:
            zf.extractall(tmp)
        except BaseException:
            shutil.rmtree(tmp, ignore_errors=True)
            raise
    tmp.rename(alvo)
    return alvo


# --------------------------------------------------------------------------
# Etapa 3 - CSV -> Parquet (streaming, RAM baixa)
# --------------------------------------------------------------------------
def detectar_encoding(caminho: Path) -> str:
    """utf-8 ou latin-1, olhando os primeiros 8 MB (os CSVs do TSE costumam ser latin-1)."""
    limite = 8 * 1024 * 1024
    with open(caminho, "rb") as f:
        amostra = f.read(limite)
    # decoder incremental: tolera um caractere UTF-8 cortado no fim da amostra,
    # mas só quando o arquivo é maior que a amostra
    decoder = codecs.getincrementaldecoder("utf-8")()
    try:
        decoder.decode(amostra, final=len(amostra) < limite)
    except UnicodeDecodeError:
        return "latin-1"
    return "utf-8"


def nomes_parquet(pasta_parquet: Path, stem: str, csvs: list[Path]) -> list[Path]:
    if len(csvs) == 1:
        return [pasta_parquet / f"{stem}.parquet"]
    return [pasta_parquet / f"{stem}__{c.stem}.parquet" for c in csvs]


def convertido(pastas: Pastas, stem: str) -> bool:
    """True se todos os .parquet desse zip já existem."""
    pasta = pastas.csv / stem
    csvs = listar_csvs(pasta) if pasta.is_dir() else []
    if csvs:  # CSV ainda presente: confere exatamente
        return all(p.exists() for p in nomes_parquet(pastas.parquet, stem, csvs))
    # CSV já apagado (--apagar-csv): só apaga depois de converter tudo
    return pastas.parquet.is_dir() and any(pastas.parquet.glob(stem + "*.parquet"))


def csv_para_parquet(csv_path: Path, destino: Path, encoding: str = "auto",
                     filtro: tuple[str, str] | None = None) -> tuple[int, int]:
    """
    Converte um CSV (separador ';') em Parquet lendo em blocos de BLOCO_CSV bytes
    e gravando um row group por bloco; a memória usada é de poucos blocos.

    filtro=(coluna, valor): grava só as linhas em que a coluna é igual ao valor
    (sem diferenciar maiúsculas/minúsculas nem espaços nas pontas). O filtro é
    aplicado bloco a bloco, então também não aumenta o uso de memória.

    Todas as colunas vão como texto (string): os CSVs do TSE têm marcadores
    como "#NULO#" em colunas numéricas e códigos com zeros à esquerda, que a
    inferência automática de tipos corromperia ou rejeitaria no meio do arquivo.
    Os tipos podem ser aplicados depois (ex.: CAST no DuckDB, astype no pandas).

    Retorna (linhas lidas, linhas gravadas).
    """
    try:
        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.csv as pacsv
        import pyarrow.parquet as pq
    except ImportError as e:
        raise RuntimeError("pyarrow não está instalado (pip install pyarrow)") from e

    enc = detectar_encoding(csv_path) if encoding == "auto" else encoding
    tmp = destino.with_name(destino.name + ".tmp")
    alvo = filtro[1].strip().lower() if filtro else None

    def passada(newlines: bool) -> tuple[int, int]:
        parse = pacsv.ParseOptions(delimiter=";", newlines_in_values=newlines)
        # 1) lê só o cabeçalho para montar o schema "tudo texto"
        with pacsv.open_csv(str(csv_path), parse_options=parse,
                            read_options=pacsv.ReadOptions(encoding=enc, block_size=1 << 20)) as r:
            nomes = list(r.schema.names)
        if filtro and filtro[0] not in nomes:
            raise ValueError(
                f"a coluna do filtro '{filtro[0]}' não existe em {csv_path.name}. "
                f"Colunas: {', '.join(nomes[:40])}. "
                f"Use --filtro COLUNA=VALOR ou --sem-filtro.")
        convert = pacsv.ConvertOptions(column_types={n: pa.string() for n in nomes},
                                       strings_can_be_null=False)
        ler = pacsv.ReadOptions(encoding=enc, block_size=BLOCO_CSV)

        lidas = gravadas = 0
        ultimo_print = 0.0
        with pacsv.open_csv(str(csv_path), read_options=ler, parse_options=parse,
                            convert_options=convert) as reader:
            with pq.ParquetWriter(str(tmp), reader.schema, compression="zstd") as writer:
                for lote in reader:
                    tabela = pa.Table.from_batches([lote])
                    lidas += tabela.num_rows
                    if filtro:
                        col = pc.utf8_lower(pc.utf8_trim_whitespace(tabela.column(filtro[0])))
                        tabela = tabela.filter(pc.equal(col, alvo))
                    if tabela.num_rows:
                        writer.write_table(tabela)
                        gravadas += tabela.num_rows
                    agora = time.monotonic()
                    if agora - ultimo_print >= 5:
                        ultimo_print = agora
                        print(f"\r  convertendo {csv_path.name}: {_n(lidas)} linhas lidas, "
                              f"{_n(gravadas)} gravadas   ", end="", flush=True)
        print("\r" + " " * 90 + "\r", end="")
        return lidas, gravadas

    for newlines in (False, True):
        try:
            lidas, gravadas = passada(newlines)
            break
        except pa.ArrowInvalid as e:
            tmp.unlink(missing_ok=True)
            # 2ª tentativa só se for erro de estrutura (ex.: quebra de linha dentro de campo)
            if newlines or "CSV parse error" not in str(e):
                raise
    tmp.replace(destino)
    return lidas, gravadas


def converter_pasta(pasta_csv: Path, stem: str, pasta_parquet: Path, encoding: str,
                    filtro: tuple[str, str] | None, fator_disco: float,
                    sobrescrever: bool) -> list[tuple[Path, int, int]]:
    csvs = listar_csvs(pasta_csv)
    if not csvs:
        raise FileNotFoundError(f"nenhum .csv encontrado em {pasta_csv}")
    pasta_parquet.mkdir(parents=True, exist_ok=True)

    feitos = []
    for csv_path, destino in zip(csvs, nomes_parquet(pasta_parquet, stem, csvs)):
        if destino.exists() and not sobrescrever:
            continue
        garantir_espaco(pasta_parquet, int(csv_path.stat().st_size * fator_disco),
                        f"converter {csv_path.name}")
        lidas, gravadas = csv_para_parquet(csv_path, destino, encoding, filtro)
        feitos.append((destino, lidas, gravadas))
    return feitos


# --------------------------------------------------------------------------
# Orquestração
# --------------------------------------------------------------------------
def processar(nome: str, pastas: Pastas, args: argparse.Namespace) -> tuple[bool, str]:
    stem = Path(nome).stem
    zip_path = pastas.zip / nome
    pasta_csv = pastas.csv / stem
    quer_parquet = "parquet" in args.etapas
    fator = FATOR_PARQUET_FILTRADO if args.filtro_par else FATOR_PARQUET
    t0 = time.monotonic()
    partes = []

    try:
        if quer_parquet and not args.reconverter and convertido(pastas, stem):
            partes.append("já convertido")
        else:
            if not pasta_csv.is_dir():
                if not zip_path.exists():
                    return False, f"[ERRO]   {stem}: zip não encontrado em {pastas.zip}"
                extrair(zip_path, pastas.csv, 1 + fator if quer_parquet else 1.0)
                partes.append("extraído")
            else:
                partes.append("CSV já extraído")
            if quer_parquet:
                for destino, lidas, gravadas in converter_pasta(
                        pasta_csv, stem, pastas.parquet, args.encoding,
                        args.filtro_par, fator, args.reconverter):
                    partes.append(f"{destino.name} ({_mb(destino.stat().st_size)}, "
                                  f"{_n(gravadas)} de {_n(lidas)} linhas)")

        if quer_parquet and convertido(pastas, stem):  # só limpa se converteu tudo
            if args.apagar_csv and pasta_csv.is_dir():
                shutil.rmtree(pasta_csv)
                partes.append("CSV apagado")
            if args.apagar_zip and zip_path.exists():
                zip_path.unlink()
                partes.append("zip apagado")
    except Exception as e:  # um arquivo com problema não derruba os demais
        return False, f"[ERRO]   {stem}: {type(e).__name__}: {e}"

    return True, f"[ok]     {stem}: {'; '.join(partes)} em {_dur(time.monotonic() - t0)}"


def executar_turno(turno: int, urls: list[str], args: argparse.Namespace,
                   base: Path) -> tuple[int, int]:
    """Retorna (nº de arquivos do turno, nº de falhas)."""
    pastas = Pastas.de(base, turno)
    print(f"\n=== {turno}º turno ===")
    if not urls:
        print(f"Nenhum arquivo do {turno}º turno encontrado na página (ainda não publicado "
              f"ou excluído pelos filtros).")
        return 0, 0
    print(f"{len(urls)} arquivo(s) .zip encontrado(s).")

    if args.listar:
        print("\n".join(urls))
        return len(urls), 0

    falhas = 0
    if "baixar" in args.etapas:
        pastas.zip.mkdir(parents=True, exist_ok=True)
        # com --apagar-zip, o que já foi convertido não precisa ser baixado de novo
        pendentes = [u for u in urls if not (args.apagar_zip and not args.reconverter
                                             and convertido(pastas, _stem(u)))]
        print(f"\n[1/3] Download -> {pastas.zip} ({len(pendentes)} arquivo(s))")
        if args.workers <= 1:
            for i, url in enumerate(pendentes, 1):
                ok, msg = baixar(url, pastas.zip, args.tentativas, progresso=True)
                print(f"({i}/{len(pendentes)}) {msg}")
                falhas += not ok
        else:
            with ThreadPoolExecutor(max_workers=args.workers) as ex:
                futuros = [ex.submit(baixar, u, pastas.zip, args.tentativas, False)
                           for u in pendentes]
                for i, fut in enumerate(as_completed(futuros), 1):
                    ok, msg = fut.result()
                    print(f"({i}/{len(pendentes)}) {msg}")
                    falhas += not ok

    if "extrair" in args.etapas or "parquet" in args.etapas:
        print(f"\n[2-3/3] Descompactar -> {pastas.csv} e converter -> {pastas.parquet}")
        for i, url in enumerate(urls, 1):
            print(f"({i}/{len(urls)}) {_stem(url)}")
            ok, msg = processar(_nome(url), pastas, args)
            print(f"        {msg}")
            falhas += not ok
    return len(urls), falhas


def _no_notebook() -> bool:
    """True no Jupyter/Colab, onde sys.argv traz parâmetros do próprio kernel."""
    return "ipykernel" in sys.modules


def main(argv: list[str] | None = None) -> int:
    """argv: lista de parâmetros, útil no Jupyter/Colab, ex.: main(["--turno", "2"]).
    Sem argv, usa os parâmetros da linha de comando."""
    turnos_validos = (1, 2)  # local: não depende de variáveis globais do notebook
    ap = argparse.ArgumentParser(
        description="Baixa, descompacta e converte para Parquet os dados do TSE 2026.")
    ap.add_argument("-b", "--base", default=".",
                    help="pasta onde as boletins_urna_* serão criadas (padrão: pasta atual)")
    ap.add_argument("-t", "--turno", choices=["1", "2", "ambos"], default="ambos",
                    help="turno a baixar/processar: 1, 2 ou ambos (padrão: ambos)")
    ap.add_argument("-d", "--dataset", action="append", metavar="ID",
                    help=f"id do dataset no portal (pode repetir). Padrão: {DATASET_ID}")
    ap.add_argument("-u", "--ufs", nargs="+", metavar="UF",
                    help="apenas essas UFs (use ZZ para o exterior)")
    ap.add_argument("--ultima-versao", action="store_true",
                    help="se uma UF tiver mais de um arquivo no mesmo turno (versões com "
                         "datas diferentes), usa só o mais recente")
    ap.add_argument("-e", "--etapas", nargs="+", choices=ETAPAS, default=list(ETAPAS),
                    help="etapas a executar (padrão: todas)")
    ap.add_argument("--filtro", default=FILTRO_PADRAO, metavar="COLUNA=VALOR",
                    help=f"o Parquet guarda só as linhas em que COLUNA = VALOR, sem "
                         f"diferenciar maiúsculas/minúsculas (padrão: {FILTRO_PADRAO})")
    ap.add_argument("--sem-filtro", action="store_true",
                    help="grava todas as linhas no Parquet")
    ap.add_argument("--reconverter", action="store_true",
                    help="refaz os .parquet mesmo que já existam (use ao mudar o filtro)")
    ap.add_argument("--apagar-zip", action="store_true",
                    help="apaga cada .zip depois de convertido (economiza disco)")
    ap.add_argument("--apagar-csv", action="store_true",
                    help="apaga os CSVs de cada .zip depois de convertido (economiza disco)")
    ap.add_argument("--encoding", default="auto",
                    help="codificação dos CSVs: auto (padrão), latin-1, utf-8...")
    ap.add_argument("-w", "--workers", type=int, default=1,
                    help="downloads simultâneos (padrão: 1); conversão é sempre 1 por vez")
    ap.add_argument("-r", "--tentativas", type=int, default=5,
                    help="tentativas por download (padrão: 5)")
    ap.add_argument("-l", "--listar", action="store_true",
                    help="apenas lista os links, sem baixar")
    args = ap.parse_args([] if argv is None and _no_notebook() else argv)

    args.filtro_par = None
    if not args.sem_filtro:
        coluna, sep, valor = args.filtro.partition("=")
        if not sep or not coluna.strip() or not valor.strip():
            ap.error("--filtro deve ter o formato COLUNA=VALOR")
        args.filtro_par = (coluna.strip(), valor.strip())

    if (args.apagar_zip or args.apagar_csv) and "parquet" not in args.etapas:
        ap.error("--apagar-zip/--apagar-csv só fazem sentido com a etapa 'parquet'")

    if "parquet" in args.etapas and not args.listar:
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            print("pyarrow não está instalado. Rode:  pip install pyarrow", file=sys.stderr)
            return 2

    if "parquet" in args.etapas and not args.listar:
        print("Filtro do Parquet: "
              + (f"{args.filtro_par[0]} = '{args.filtro_par[1]}'" if args.filtro_par
                 else "nenhum (todas as linhas)"))

    if args.dataset:
        datasets = args.dataset
    else:
        datasets = [DATASET_ID]
    print("Buscando a lista de arquivos em: "
          + ", ".join(f"{BASE_URL}/dataset/{d}" for d in datasets))
    try:
        por_turno, fora_do_padrao = agrupar_por_turno(listar_urls(datasets))
    except OSError as e:
        print(f"[ERRO] não consegui listar os arquivos: {e}")
        return 1
    if fora_do_padrao:
        print(f"[aviso] {len(fora_do_padrao)} arquivo(s) .zip fora do padrão "
              f"bweb_<turno>t_<UF>_<data>.zip foram ignorados: "
              + ", ".join(_nome(u) for u in fora_do_padrao[:5]))

    ufs = {u.upper() for u in args.ufs} if args.ufs else None
    base = Path(args.base)
    print("Arquivos encontrados por turno: "
          + ", ".join(f"{t}º = {len(por_turno.get(t, []))}" for t in turnos_validos))
    turnos = list(turnos_validos) if args.turno == "ambos" else [int(args.turno)]
    total, falhas = 0, 0
    for turno in turnos:
        urls = filtrar(por_turno.get(turno, []), ufs, args.ultima_versao)
        n, f = executar_turno(turno, urls, args, base)
        total, falhas = total + n, falhas + f

    if args.listar:
        return 0 if total else 1
    print(f"\nConcluído: {total} arquivo(s), {falhas} com erro."
          + (" Rode de novo para tentar novamente só o que falhou." if falhas else ""))
    return 1 if (falhas or not total) else 0


if __name__ == "__main__":
    try:
        codigo = main()
        #  main(["--turno", "2"])
    except KeyboardInterrupt:
        print("\nInterrompido. Rode novamente para retomar de onde parou.")
        codigo = 130
    # No Jupyter/Colab, sys.exit() aparece como "exceção SystemExit" mesmo com
    # sucesso (código 0); lá só mostramos o código. No terminal, sai normalmente.
    if _no_notebook():
        if codigo:
            print(f"(código de saída: {codigo})")
    else:
        sys.exit(codigo)
