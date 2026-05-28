import logging

from indiv_source_runner import run_source


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


if __name__ == "__main__":
    output_dir = run_source("OS1", "output/indiv_os1")
    print(f"OS1 reports generated under: {output_dir}")
