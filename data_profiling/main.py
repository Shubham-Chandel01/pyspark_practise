from profiler import generate_profile_report
from config.config import load_config

#  Extracts input and output file paths from the configuration.
def get_file_paths(config):
    input_file = config['movies_gross_file_path']
    output_file = config["movies_profile_output_path"]
    return input_file, output_file

def main():
    """Main function to run data profiling."""

    config = load_config("../config/config.yaml")
    input_file, output_file = get_file_paths(config)
    # Generate the profiling report
    generate_profile_report(input_file, output_file)


if __name__ == "__main__":
    main()