from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ScraperDef:
    id: str
    name: str
    domain: str
    script_file: str
    output_csv: str
    partial_csv: str
    cleaned_csv: str
    default_sep: str
    keywords: list[str]
    countries: list[str]
    archive_csvs: tuple[str, ...] = ()

    @property
    def csv_artifacts(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys((
            self.output_csv,
            self.partial_csv,
            self.cleaned_csv,
            *self.archive_csvs,
        )))


SCRAPERS: dict[str, ScraperDef] = {
    "tradewheel": ScraperDef(
        id="tradewheel",
        name="Tradewheel",
        domain="tradewheel.com",
        script_file="tradewheel_scraper.py",
        output_csv="tradewheel_suppliers_raw.csv",
        partial_csv="tradewheel_suppliers_partial.csv",
        cleaned_csv="tradewheel_suppliers_cleaned.csv",
        default_sep="\t",
        keywords=["cosmetic tubes", "cosmetic bottles", "cosmetic jars", "cosmetic packaging"],
        countries=["China", "South Korea", "Taiwan", "Japan", "Vietnam", "Thailand"],
    ),
    "kompass": ScraperDef(
        id="kompass",
        name="Kompass",
        domain="kompass.com",
        script_file="kompass_scraper_final.py",
        output_csv="kompass_suppliers_phase1_raw.csv",
        partial_csv="kompass_suppliers_phase1_partial.csv",
        cleaned_csv="kompass_suppliers_cleaned.csv",
        default_sep=",",
        keywords=["cosmetic packaging", "beauty packaging"],
        countries=["China", "South Korea", "Taiwan", "Japan", "Vietnam"],
        archive_csvs=(
            "kompass_suppliers_enriched.csv",
            "kompass_suppliers_enrichment_checkpoint.csv",
        ),
    ),
    "made_in_china": ScraperDef(
        id="made_in_china",
        name="Made-in-China",
        domain="made-in-china.com",
        script_file="made_in_china_scraper_final.py",
        output_csv="made_in_china_suppliers_phase1_raw.csv",
        partial_csv="made_in_china_suppliers_partial_enrichment.csv",
        cleaned_csv="made_in_china_suppliers_cleaned.csv",
        default_sep="\t",
        keywords=["cosmetic tubes", "cosmetic bottles", "airless pumps"],
        countries=["China"],
        archive_csvs=("made_in_china_suppliers_partial_scrape.csv",),
    ),
    "ec21": ScraperDef(
        id="ec21",
        name="EC21",
        domain="ec21.com",
        script_file="ec21_scraper_final.py",
        output_csv="ec21_suppliers_phase1_raw.csv",
        partial_csv="ec21_suppliers_enrich_progress.csv",
        cleaned_csv="ec21_suppliers_cleaned.csv",
        default_sep="\t",
        keywords=["cosmetic-packaging", "cosmetic-bottles", "cosmetic-tubes"],
        countries=["China", "South Korea", "Taiwan", "Japan", "Vietnam", "Thailand"],
        archive_csvs=("ec21_suppliers_scrape_progress.csv",),
    ),
    "exportpages": ScraperDef(
        id="exportpages",
        name="ExportPages",
        domain="exportpages.com",
        script_file="exportpages_scraper_final.py",
        output_csv="exportpages_suppliers_raw.csv",
        partial_csv="exportpages_suppliers_enrich_progress.csv",
        cleaned_csv="exportpages_suppliers_cleaned.csv",
        default_sep="\t",
        keywords=["category 142", "cosmetic packaging"],
        countries=["China", "South Korea", "Taiwan", "Japan", "Vietnam", "Thailand"],
        archive_csvs=("exportpages_suppliers_scrape_progress.csv",),
    ),
}
