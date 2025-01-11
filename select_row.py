import argparse
from run import ParkingDataProcessor, setup_logging, load_config

def main():
    # Налаштування парсингу аргументів командного рядка
    parser = argparse.ArgumentParser(description="Select parking data by pay_id.")
    parser.add_argument('pay_id', type=int, help="The pay_id to select")
    args = parser.parse_args()

    # Налаштування логування та конфігурації
    setup_logging()
    config = load_config()

    # Обробка даних
    processor = ParkingDataProcessor(config)
    row = processor.select_by_id(args.pay_id)
    print(row)


if __name__ == '__main__':
    main()
