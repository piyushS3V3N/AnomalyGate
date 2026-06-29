import argparse
import sys

def main():
    parser = argparse.ArgumentParser(description="Automated Security Log Noise Filter")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Generate data command
    generate_parser = subparsers.add_parser("generate", help="Generate synthetic log data")
    generate_parser.add_argument("num", help="Number of logs to generate", default=10000, type=int, nargs="?")
    
    # Train command
    subparsers.add_parser("train", help="Train the PySpark ML Model")
    
    # Run pipeline command
    subparsers.add_parser("run", help="Run the streaming pipeline (Kafka -> PySpark -> Elasticsearch)")

    args = parser.parse_args()

    if args.command == "generate":
        from src.utils.data_generator import run_generation
        num_logs = getattr(args, "num", 10000)
        run_generation(num_logs)
    elif args.command == "train":
        from src.ml.train import train_model
        train_model()
    elif args.command == "run":
        from src.pipeline.stream import run_pipeline
        run_pipeline()
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()
