import logging

from indiv_source_runner import run_source


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


if __name__ == "__main__":
    output_dir = run_source("MIS", "output/indiv_mis")
    print(f"MIS reports generated under: {output_dir}")
