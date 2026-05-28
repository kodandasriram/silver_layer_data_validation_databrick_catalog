import logging

from indiv_source_runner import run_source


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


if __name__ == "__main__":
    output_dir = run_source("OS2", "output/indiv_os2")
    print(f"OS2 reports generated under: {output_dir}")
