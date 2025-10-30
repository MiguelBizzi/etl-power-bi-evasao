import csv
import os
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).parent
INPUT_DIR = BASE_DIR / "input-csv"
PROCESSED_DIR = INPUT_DIR / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# Mappings
UF_NAME_TO_CODE = {
    "Acre": "AC", "Alagoas": "AL", "Amapá": "AP", "Amazonas": "AM",
    "Bahia": "BA", "Ceará": "CE", "Distrito Federal": "DF", "Espírito Santo": "ES",
    "Goiás": "GO", "Maranhão": "MA", "Mato Grosso": "MT", "Mato Grosso do Sul": "MS",
    "Minas Gerais": "MG", "Pará": "PA", "Paraíba": "PB", "Paraná": "PR",
    "Pernambuco": "PE", "Piauí": "PI", "Rio de Janeiro": "RJ", "Rio Grande do Norte": "RN",
    "Rio Grande do Sul": "RS", "Rondônia": "RO", "Roraima": "RR", "Santa Catarina": "SC",
    "São Paulo": "SP", "Sergipe": "SE", "Tocantins": "TO"
}

UF_TO_REGION = {
    "AC": "Norte", "AP": "Norte", "AM": "Norte", "PA": "Norte", "RO": "Norte", "RR": "Norte", "TO": "Norte",
    "AL": "Nordeste", "BA": "Nordeste", "CE": "Nordeste", "MA": "Nordeste", "PB": "Nordeste", "PE": "Nordeste", "PI": "Nordeste", "RN": "Nordeste", "SE": "Nordeste",
    "DF": "Centro-Oeste", "GO": "Centro-Oeste", "MT": "Centro-Oeste", "MS": "Centro-Oeste",
    "ES": "Sudeste", "MG": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
    "PR": "Sul", "RS": "Sul", "SC": "Sul"
}

REGION_TO_UFS = {}
for uf, reg in UF_TO_REGION.items():
    REGION_TO_UFS.setdefault(reg, []).append(uf)


def _to_float(value: str) -> float:
    if value is None:
        return 0.0
    v = value.strip().replace("\u00A0", " ")
    if v == "" or v.upper() in {"NA", "NULL", "-"}:
        return 0.0
    # Handle Brazilian decimal comma if present
    v = v.replace(".", "").replace(",", ".") if ";" not in v and v.count(",") == 1 and v.count(".") > 1 else v
    try:
        return float(v.replace(",", "."))
    except ValueError:
        return 0.0


def process_inep_rendimento():
    src = INPUT_DIR / "tx_rend_brasil_regioes_ufs_2024.csv"
    out = PROCESSED_DIR / "inep_rendimento.csv"

    headers = None
    # Target columns
    col_map = {
        "ano": "NU_ANO_CENSO",
        "unidgeo": "UNIDGEO",
        "localizacao": "NO_CATEGORIA",
        "dependencia": "NO_DEPENDENCIA",
        "aprov_fun": "1_CAT_FUN",
        "reprov_fun": "2_CAT_FUN",
        "aband_fun": "3_CAT_FUN",
        "aprov_med": "1_CAT_MED",
        "reprov_med": "2_CAT_MED",
        "aband_med": "3_CAT_MED",
    }

    with src.open("r", encoding="utf-8-sig", newline="") as f_in, out.open("w", encoding="utf-8", newline="") as f_out:
        reader = csv.reader(f_in)
        writer = csv.writer(f_out, delimiter=";")
        writer.writerow(["ano", "regiao", "uf", "dependencia", "nivel", "taxa_aprovacao", "taxa_reprovacao", "taxa_abandono"])

        for row in reader:
            if not row:
                continue
            # Find the real header
            if headers is None and "NU_ANO_CENSO" in row:
                headers = {name.strip(): idx for idx, name in enumerate(row)}
                # Ensure all columns exist
                missing = [v for v in col_map.values() if v not in headers]
                if missing:
                    # If the structure changes, skip until found
                    headers = None
                continue
            if headers is None:
                continue

            def g(col_name: str) -> str:
                idx = headers.get(col_name, -1)
                return row[idx].strip() if 0 <= idx < len(row) else ""

            ano = g(col_map["ano"])
            unidgeo = g(col_map["unidgeo"]).strip()
            localizacao = g(col_map["localizacao"]).strip()
            dependencia = g(col_map["dependencia"]).strip()

            # Keep only Localização Total e UFs
            if localizacao != "Total":
                continue

            uf_code = UF_NAME_TO_CODE.get(unidgeo)
            if not uf_code:
                # skip Brasil/Regiões e outros agregados
                continue

            regiao = UF_TO_REGION.get(uf_code, "")

            # Fundamental
            aprov_fun = _to_float(g(col_map["aprov_fun"]))
            reprov_fun = _to_float(g(col_map["reprov_fun"]))
            aband_fun = _to_float(g(col_map["aband_fun"]))
            if any([aprov_fun, reprov_fun, aband_fun]):
                writer.writerow([ano, regiao, uf_code, dependencia, "Fundamental", f"{aprov_fun:.2f}", f"{reprov_fun:.2f}", f"{aband_fun:.2f}"])

            # Médio
            aprov_med = _to_float(g(col_map["aprov_med"]))
            reprov_med = _to_float(g(col_map["reprov_med"]))
            aband_med = _to_float(g(col_map["aband_med"]))
            if any([aprov_med, reprov_med, aband_med]):
                writer.writerow([ano, regiao, uf_code, dependencia, "Medio", f"{aprov_med:.2f}", f"{reprov_med:.2f}", f"{aband_med:.2f}"])


def process_inep_distorcao():
    src = INPUT_DIR / "TDI_BRASIL_REGIOES_UFS_2024.csv"
    out = PROCESSED_DIR / "inep_distorcao.csv"

    headers = None
    col_map = {
        "ano": "NU_ANO_CENSO",
        "unidgeo": "UNIDGEO",
        "localizacao": "NO_CATEGORIA",
        "dependencia": "NO_DEPENDENCIA",
        "disto_fun": "FUN_CAT_0",
        "disto_med": "MED_CAT_0",
    }

    with src.open("r", encoding="utf-8-sig", newline="") as f_in, out.open("w", encoding="utf-8", newline="") as f_out:
        reader = csv.reader(f_in)
        writer = csv.writer(f_out, delimiter=";")
        writer.writerow(["ano", "regiao", "uf", "dependencia", "nivel", "taxa_distorcao"])

        for row in reader:
            if not row:
                continue
            if headers is None and "NU_ANO_CENSO" in row:
                headers = {name.strip(): idx for idx, name in enumerate(row)}
                missing = [v for v in col_map.values() if v not in headers]
                if missing:
                    headers = None
                continue
            if headers is None:
                continue

            def g(col_name: str) -> str:
                idx = headers.get(col_name, -1)
                return row[idx].strip() if 0 <= idx < len(row) else ""

            ano = g(col_map["ano"])
            unidgeo = g(col_map["unidgeo"]).strip()
            localizacao = g(col_map["localizacao"]).strip()
            dependencia = g(col_map["dependencia"]).strip()

            if localizacao != "Total":
                continue

            uf_code = UF_NAME_TO_CODE.get(unidgeo)
            if not uf_code:
                continue
            regiao = UF_TO_REGION.get(uf_code, "")

            disto_fun = _to_float(g(col_map["disto_fun"]))
            if disto_fun:
                writer.writerow([ano, regiao, uf_code, dependencia, "Fundamental", f"{disto_fun:.2f}"])

            disto_med = _to_float(g(col_map["disto_med"]))
            if disto_med:
                writer.writerow([ano, regiao, uf_code, dependencia, "Medio", f"{disto_med:.2f}"])


def process_ibge_analfabetismo():
    src_dir = INPUT_DIR / "tabelas-analfabetismo"
    out = PROCESSED_DIR / "ibge_analfabetismo.csv"

    rows_out = []

    if not src_dir.exists():
        # Backward compatibility: fall back to single file if directory missing
        single = INPUT_DIR / "Tabela 4.13 (TaxaAnalf_Geo).csv"
        if single.exists():
            years = {"2023": single}
        else:
            years = {}
    else:
        # Gather all TabelaYYYY.csv files
        years = {}
        for p in src_dir.glob("Tabela*.csv"):
            # Extract year from filename, e.g., Tabela2016.csv
            stem = p.stem
            year = ''.join(ch for ch in stem if ch.isdigit())
            if len(year) == 4:
                years[year] = p

    for year, path in sorted(years.items()):
        region_rates = {}
        with path.open("r", encoding="utf-8-sig", newline="") as f_in:
            reader = csv.reader(f_in)
            for row in reader:
                if not row or not row[0]:
                    continue
                name = row[0].strip().replace("  ", " ")
                # Normalize Brasil spelling/spaces
                if name.startswith("Brasil"):
                    name = "Brasil"
                if name in {"Brasil", "Norte", "Nordeste", "Sudeste", "Sul", "Centro-Oeste"}:
                    value = None
                    for cell in row[1:]:
                        cell_s = (cell or "").strip()
                        if cell_s:
                            value = cell_s
                            break
                    if value is None:
                        continue
                    region_rates[name] = _to_float(value)

        # Expand rates to UFs of each region
        for region, ufs in REGION_TO_UFS.items():
            rate = region_rates.get(region)
            if rate is None:
                continue
            for uf in sorted(ufs):
                rows_out.append([year, region, uf, f"{rate:.2f}"])

    # Write output
    with out.open("w", encoding="utf-8", newline="") as f_out:
        writer = csv.writer(f_out, delimiter=";")
        writer.writerow(["ano", "regiao", "uf", "taxa_analfabetismo"])
        writer.writerows(rows_out)


def process_ibge_motivos():
    src_dir = INPUT_DIR / "tabelas-motivos"
    single_fallback = INPUT_DIR / "Tabela 4.17 (SemEnsinoMedioMotivParar_BR).csv"
    out = PROCESSED_DIR / "ibge_motivos.csv"

    # Collect (ano, motivo, sexo, faixa_etaria, valor)
    records = []

    files = []
    if src_dir.exists():
        for p in src_dir.glob("Tabela*.csv"):
            stem = p.stem
            year = ''.join(ch for ch in stem if ch.isdigit())
            if len(year) == 4:
                files.append((year, p))
        files.sort(key=lambda x: x[0])
    elif single_fallback.exists():
        files = [("2023", single_fallback)]

    phrase = "Motivos de ter parado de frequentar escola (%)"

    for year, path in files:
        with path.open("r", encoding="utf-8-sig", newline="") as f_in:
            rows = list(csv.reader(f_in))

        motives_header_row = None
        motives_start_idx = None
        totals_row = None
        data_rows = []

        # Find phrase row, next row is header of motives
        header_index = None
        for i, row in enumerate(rows):
            if not row:
                continue
            joined = ",".join([c or "" for c in row])
            if phrase in joined:
                header_index = i
                break

        if header_index is not None:
            for j in range(header_index + 1, len(rows)):
                cand = rows[j]
                if cand and any(cell.strip() for cell in cand):
                    motives_header_row = cand
                    break

        if motives_header_row:
            for idx in range(2, len(motives_header_row)):
                if motives_header_row[idx].strip():
                    motives_start_idx = idx
                    break

        # First row starting with 'Total'
        for row in rows:
            if row and row[0].strip().startswith("Total"):
                totals_row = row
                break

        # Gather all non-empty rows after header for potential breakdowns
        if motives_header_row and motives_start_idx is not None:
            header_found = False
            for row in rows:
                if not header_found:
                    if row is motives_header_row:
                        header_found = True
                    continue
                if not row or not any(cell.strip() for cell in row):
                    continue
                data_rows.append(row)

        def normalize_sex(label: str) -> str | None:
            l = label.strip().lower()
            if l.startswith("homens") or l == "masculino":
                return "Masculino"
            if l.startswith("mulheres") or l == "feminino":
                return "Feminino"
            return None

        def is_age_band(label: str) -> bool:
            l = label.strip().lower()
            if l.startswith("faixa et"):
                return False
            if l.startswith("total"):
                return False
            return any(ch.isdigit() for ch in l) and (" a " in l or "anos" in l)

        # 1) Totais gerais
        if motives_header_row and motives_start_idx is not None and totals_row:
            for idx in range(motives_start_idx, len(motives_header_row)):
                name = motives_header_row[idx].strip()
                if not name:
                    continue
                val = _to_float(totals_row[idx] if idx < len(totals_row) else "")
                records.append([year, name, "Total", "Total", f"{val:.2f}"])

        # 2) Quebra por sexo (linhas cujo rótulo indica sexo)
        for row in data_rows:
            label = (row[0] or "").strip()
            sex = normalize_sex(label)
            if not sex:
                continue
            for idx in range(motives_start_idx, len(motives_header_row)):
                name = motives_header_row[idx].strip()
                if not name:
                    continue
                val = _to_float(row[idx] if idx < len(row) else "")
                records.append([year, name, sex, "Total", f"{val:.2f}"])

        # 3) Quebra por faixa etária (linhas que parecem faixa etária)
        for row in data_rows:
            label = (row[0] or "").strip()
            if not is_age_band(label):
                continue
            faixa = label
            for idx in range(motives_start_idx, len(motives_header_row)):
                name = motives_header_row[idx].strip()
                if not name:
                    continue
                val = _to_float(row[idx] if idx < len(row) else "")
                records.append([year, name, "Total", faixa, f"{val:.2f}"])

    # Write output merged for all years
    with out.open("w", encoding="utf-8", newline="") as f_out:
        writer = csv.writer(f_out, delimiter=";")
        writer.writerow(["ano", "motivo", "sexo", "faixa_etaria", "percentual"])
        writer.writerows(records)


def main():
    process_inep_rendimento()
    process_inep_distorcao()
    process_ibge_analfabetismo()
    process_ibge_motivos()
    print(f"Arquivos gerados em: {PROCESSED_DIR}")


if __name__ == "__main__":
    main()


