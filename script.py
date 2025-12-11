import csv
import os
import random
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


def mock_process_ibge_analfabetismo():
    """Mock process that generates ibge_analfabetismo.csv with random values between 0 and 20."""
    out = PROCESSED_DIR / "ibge_analfabetismo.csv"
    
    years = ["2016", "2017", "2018", "2019", "2022", "2023"]
    
    rows_out = []
    
    for year in years:
        for region, ufs in sorted(REGION_TO_UFS.items()):
            for uf in sorted(ufs):
                rate = round(random.uniform(0, 20), 2)
                rows_out.append([year, region, uf, f"{rate:.2f}"])
    
    with out.open("w", encoding="utf-8", newline="") as f_out:
        writer = csv.writer(f_out, delimiter=";")
        writer.writerow(["ano", "regiao", "uf", "taxa_analfabetismo"])
        writer.writerows(rows_out)


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
    # Agora restringimos a Sexo (Masculino/Feminino) e Faixas (15 a 17, 18 a 24, 25 a 29)
    # e, caso não exista cruzamento explícito sexo×faixa, estimamos via alocação proporcional
    # usando as marginais de Sexo e de Faixa para cada motivo/ano, normalizando para o total do motivo.
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

    TARGET_SEX_LABELS = ["Masculino", "Feminino"]
    TARGET_AGE_LABELS = ["15 a 17 anos", "18 a 24 anos", "25 a 29 anos"]

    for year, path in files:
        with path.open("r", encoding="utf-8-sig", newline="") as f_in:
            rows = list(csv.reader(f_in))

        motives_header_row = None
        motives_start_idx = None
        totals_row = None

        # Armazenar marginais por sexo e por faixa (por motivo)
        sex_marginals: dict[str, dict[str, float]] = {"Masculino": {}, "Feminino": {}}
        age_marginals: dict[str, dict[str, float]] = {age: {} for age in TARGET_AGE_LABELS}

        # Find phrase row, next non-empty row is header of motives
        phrase_row_idx = None
        header_row_idx = None
        for i, row in enumerate(rows):
            if not row:
                continue
            joined = ",".join([c or "" for c in row])
            if phrase in joined:
                phrase_row_idx = i
                break

        if phrase_row_idx is not None:
            for j in range(phrase_row_idx + 1, len(rows)):
                cand = rows[j]
                if cand and any((cell or "").strip() for cell in cand):
                    motives_header_row = cand
                    header_row_idx = j
                    break

        if motives_header_row:
            for idx in range(2, len(motives_header_row)):
                if (motives_header_row[idx] or "").strip():
                    motives_start_idx = idx
                    break

        # First 'Total' row after header
        if header_row_idx is not None:
            for k in range(header_row_idx + 1, len(rows)):
                r0 = (rows[k][0] or "").strip() if rows[k] else ""
                if r0.startswith("Total"):
                    totals_row = rows[k]
                    break

        def normalize_sex(label: str) -> str | None:
            l = label.strip().lower()
            if l.startswith("homem") or l == "masculino":
                return "Masculino"
            if l.startswith("mulher") or l == "feminino":
                return "Feminino"
            return None

        def is_age_band(label: str) -> bool:
            l = label.strip().lower()
            if l.startswith("faixa et"):
                return False
            if l.startswith("total"):
                return False
            return any(ch.isdigit() for ch in l) and (" a " in l or "anos" in l)

        if motives_header_row is None or motives_start_idx is None:
            continue

        # 1) Capturar totais gerais por motivo (percentual total do motivo no universo)
        totals_by_motive: dict[str, float] = {}
        if totals_row:
            for idx in range(motives_start_idx, len(motives_header_row)):
                name = (motives_header_row[idx] or "").strip()
                if not name:
                    continue
                val = _to_float(totals_row[idx] if idx < len(totals_row) else "")
                totals_by_motive[name] = val

        # 2) Varredura por seções: Sexo e Grupos de idade
        mode = None  # None | 'sexo' | 'faixa'
        for r in range(header_row_idx + 1, len(rows)):
            row = rows[r]
            if not row:
                continue
            first = (row[0] or "").strip()
            if not first:
                continue
            # Detecta início de seções
            if first.lower().startswith("sexo"):
                mode = 'sexo'
                continue
            if first.lower().startswith("grupos de idade"):
                mode = 'faixa'
                continue
            # Mudança para outras seções: interrompe modo
            if first.lower().startswith("cor ou raça") or first.lower().startswith("cor ou raça e sexo") or first.lower().startswith("situação de atividade") or first.lower().startswith("classes de percentual") or first.lower().startswith("nível de instrução") or first.lower().startswith("condição na unidade") or first.lower().startswith("situação do domicílio") or first.lower().startswith("grupos de idade com que") or first.lower().startswith("fonte") or first.lower().startswith("notas"):
                mode = None
                continue

            if mode == 'sexo':
                sex = normalize_sex(first)
                if sex in TARGET_SEX_LABELS:
                    for idx in range(motives_start_idx, len(motives_header_row)):
                        name = (motives_header_row[idx] or "").strip()
                        if not name:
                            continue
                        val = _to_float(row[idx] if idx < len(row) else "")
                        sex_marginals[sex][name] = val
                continue

            if mode == 'faixa' and is_age_band(first):
                faixa = first
                if faixa in TARGET_AGE_LABELS:
                    for idx in range(motives_start_idx, len(motives_header_row)):
                        name = (motives_header_row[idx] or "").strip()
                        if not name:
                            continue
                        val = _to_float(row[idx] if idx < len(row) else "")
                        age_marginals[faixa][name] = val

        # 3) Geração do cruzamento sexo×faixa por motivo via:
        #    - dados explícitos (se existirem — não há nessas tabelas), ou
        #    - fallback por alocação proporcional com base nas marginais e no total do motivo
        motive_names: list[str] = []
        if motives_header_row and motives_start_idx is not None:
            for idx in range(motives_start_idx, len(motives_header_row)):
                nm = (motives_header_row[idx] or "").strip()
                if nm:
                    motive_names.append(nm)

        for motive in motive_names:
            total_pct = totals_by_motive.get(motive, 0.0)
            if total_pct <= 0:
                continue

            male = sex_marginals.get("Masculino", {}).get(motive, 0.0)
            female = sex_marginals.get("Feminino", {}).get(motive, 0.0)
            a15 = age_marginals.get("15 a 17 anos", {}).get(motive, 0.0)
            a18 = age_marginals.get("18 a 24 anos", {}).get(motive, 0.0)
            a25 = age_marginals.get("25 a 29 anos", {}).get(motive, 0.0)

            # Pesos iniciais: produto externo das marginais (independência),
            # normalizados para que a soma dos 6 seja 1, depois multiplicamos por total_pct
            row = [max(male, 0.0), max(female, 0.0)]
            col = [max(a15, 0.0), max(a18, 0.0), max(a25, 0.0)]
            weights = []
            for rv in row:
                for cv in col:
                    weights.append(rv * cv)
            s = sum(weights)
            if s <= 0:
                # Caso extremo: dividir igualmente entre as 6 células
                weights = [1.0] * 6
                s = 6.0
            weights = [w / s for w in weights]

            # Mapear de volta para (sexo, faixa)
            combos = [
                ("Masculino", "15 a 17 anos"), ("Masculino", "18 a 24 anos"), ("Masculino", "25 a 29 anos"),
                ("Feminino", "15 a 17 anos"), ("Feminino", "18 a 24 anos"), ("Feminino", "25 a 29 anos"),
            ]
            for (sex, faixa), w in zip(combos, weights):
                pct = total_pct * w
                records.append([year, motive, sex, faixa, f"{pct:.2f}"])

    # Write output merged for all years
    with out.open("w", encoding="utf-8", newline="") as f_out:
        writer = csv.writer(f_out, delimiter=";")
        writer.writerow(["ano", "motivo", "sexo", "faixa_etaria", "percentual"])
        writer.writerows(records)


def main():
    process_inep_rendimento()
    process_inep_distorcao()
    mock_process_ibge_analfabetismo()
    process_ibge_motivos()
    print(f"Arquivos gerados em: {PROCESSED_DIR}")


if __name__ == "__main__":
    main()


