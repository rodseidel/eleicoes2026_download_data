# Boletins de Urna 2026 (TSE) → Parquet

Script em Python que **baixa, descompacta e converte para Parquet** os boletins de urna das eleições 2026 (1º e 2º turnos), publicados no [Portal de Dados Abertos do TSE](https://dadosabertos.tse.jus.br/dataset/resultados-2026-boletim-de-urna).

Os arquivos brutos do TSE são ZIPs grandes, com CSVs enormes, separados por UF. O script automatiza todo o caminho até um conjunto de arquivos `.parquet` prontos para análise (DuckDB, pandas, Polars, Spark etc.), usando pouca memória e podendo ser interrompido e retomado a qualquer momento.

## Features

- **Descoberta automática dos links** pela API CKAN do portal, com fallback para o HTML da página. Só os `.zip` de dados entram; os arquivos de hash (`.sha512`) são ignorados.
- **Identificação do turno pelo nome do arquivo** (`bweb_1t_<UF>_...`, `bweb_2t_<UF>_...`). Quando o TSE publicar os arquivos do 2º turno, basta rodar o script novamente.
- **Download com retomada** (`.part`), tentativas automáticas com espera progressiva e opção de downloads simultâneos.
- **Execução idempotente**: o que já foi baixado, extraído ou convertido é pulado. É seguro interromper com `Ctrl+C` e rodar de novo.
- **Conversão em streaming**: o CSV é lido em blocos e o Parquet é gravado aos poucos, então a RAM usada não depende do tamanho do arquivo. Compressão `zstd`.
- **Filtro de linhas no Parquet** (padrão: `DS_CARGO_PERGUNTA = Presidente`, sem diferenciar maiúsculas/minúsculas). Pode ser alterado ou desligado. O CSV extraído permanece completo.
- **Todas as colunas gravadas como texto**, para preservar zeros à esquerda e marcadores como `#NULO#`. Os tipos podem ser aplicados depois.
- **Detecção automática de encoding** (UTF-8 ou Latin-1).
- **Controle de espaço em disco**: verifica o espaço livre antes de extrair e converter, e permite apagar ZIPs e CSVs após a conversão.
- **Filtros de seleção**: por turno, por UF e apenas a versão mais recente de cada UF.
- **Etapas independentes**: é possível executar só o download, só a extração ou só a conversão.

## Requisitos

- Python 3.8+
- Download e descompactação usam apenas a biblioteca padrão
- Conversão para Parquet requer o [`pyarrow`](https://arrow.apache.org/docs/python/):

```bash
pip install pyarrow
```

## Estrutura de saída

As pastas são criadas dentro do diretório indicado em `--base` (padrão: pasta atual):

```
boletins_urna_1turno/            # .zip baixados
boletins_urna_1turno_csv/        # CSVs extraídos (uma subpasta por .zip)
boletins_urna_1turno_parquet/    # um .parquet por .zip
boletins_urna_2turno/            # (mesma estrutura para o 2º turno)
boletins_urna_2turno_csv/
boletins_urna_2turno_parquet/
```

## Como usar

### 1. Linha de comando

```bash
# Lista os links encontrados, sem baixar nada
python download_data_bu_2026.py --listar

# Teste pequeno: 1º turno, apenas o Acre
python download_data_bu_2026.py --turno 1 --ufs AC

# Apenas o 2º turno
python download_data_bu_2026.py --turno 2

# Confere os links já publicados do 2º turno
python download_data_bu_2026.py --turno 2 --listar

# Tudo (1º e 2º turnos, todas as UFs)
python download_data_bu_2026.py

# Economiza disco: apaga ZIP e CSV após converter cada arquivo
python download_data_bu_2026.py --apagar-zip --apagar-csv

# Apenas baixa, sem descompactar nem converter
python download_data_bu_2026.py --etapas baixar

# Refaz os .parquet (use ao mudar o filtro)
python download_data_bu_2026.py --reconverter

# Parquet com todas as linhas, 4 downloads simultâneos
python download_data_bu_2026.py --sem-filtro --workers 4

# Parquet com outro filtro
python download_data_bu_2026.py --filtro "DS_CARGO_PERGUNTA=Governador" --reconverter
```

### 2. Jupyter / Google Colab

Em notebooks, `sys.argv` traz parâmetros do kernel, então os argumentos são passados como lista para `main()`:

```python
%run download_data_bu_2026.py          # executa com os padrões (tudo)
```

```python
from download_data_bu_2026 import main

main(["--turno", "2"])
main(["--turno", "1", "--ufs", "AC", "SP", "--workers", "4"])
```

### 3. Como módulo em outro script Python

```python
from download_data_bu_2026 import main

codigo = main(["--turno", "2", "--base", "dados/tse", "--apagar-zip"])
```

`main()` retorna o código de saída (`0` em caso de sucesso).

## Opções

| Opção | Descrição | Padrão |
|---|---|---|
| `-b`, `--base` | Pasta onde as `boletins_urna_*` serão criadas | `.` |
| `-t`, `--turno` | Turno a processar: `1`, `2` ou `ambos` | `ambos` |
| `-d`, `--dataset` | ID do dataset no portal (pode repetir) | `resultados-2026-boletim-de-urna` |
| `-u`, `--ufs` | Apenas essas UFs (use `ZZ` para o exterior) | todas |
| `--ultima-versao` | Se uma UF tiver mais de um arquivo no turno, usa só o mais recente | desligado |
| `-e`, `--etapas` | Etapas a executar: `baixar`, `extrair`, `parquet` | todas |
| `--filtro COLUNA=VALOR` | O Parquet guarda só as linhas em que COLUNA = VALOR | `DS_CARGO_PERGUNTA=Presidente` |
| `--sem-filtro` | Grava todas as linhas no Parquet | desligado |
| `--reconverter` | Refaz os `.parquet` mesmo que já existam | desligado |
| `--apagar-zip` | Apaga cada `.zip` depois de convertido | desligado |
| `--apagar-csv` | Apaga os CSVs de cada `.zip` depois de convertido | desligado |
| `--encoding` | Codificação dos CSVs: `auto`, `latin-1`, `utf-8`... | `auto` |
| `-w`, `--workers` | Downloads simultâneos (a conversão é sempre um arquivo por vez) | `1` |
| `-r`, `--tentativas` | Tentativas por download | `5` |
| `-l`, `--listar` | Apenas lista os links, sem baixar | desligado |

> `--apagar-zip` e `--apagar-csv` exigem que a etapa `parquet` esteja ativa.

## Lendo os resultados

Como todas as colunas são texto, aplique os tipos na leitura.

**DuckDB**

```sql
SELECT *
FROM 'boletins_urna_1turno_parquet/*.parquet'
LIMIT 10;
```

**pandas**

```python
import pandas as pd

df = pd.read_parquet("boletins_urna_1turno_parquet")
```

## Códigos de saída

| Código | Significado |
|---|---|
| `0` | Concluído sem erros |
| `1` | Houve falhas, ou nenhum arquivo foi encontrado |
| `2` | `pyarrow` não instalado (necessário para a etapa `parquet`) |
| `130` | Interrompido pelo usuário (`Ctrl+C`) |

Em caso de falhas, rode o script novamente: apenas o que falhou será refeito.

## Observações

- O espaço livre em disco é verificado antes de extrair e converter (com margem mínima de 2 GB). Se faltar espaço, use `--apagar-zip` e `--apagar-csv`.
- Arquivos `.zip` fora do padrão `bweb_<turno>t_<UF>_<carimbo>.zip` são ignorados, e um aviso é exibido.
- Se mudar o `--filtro`, use também `--reconverter`, senão os `.parquet` existentes serão mantidos.

## Fonte dos dados

Portal de Dados Abertos do TSE: <https://dadosabertos.tse.jus.br/dataset/resultados-2026-boletim-de-urna>
