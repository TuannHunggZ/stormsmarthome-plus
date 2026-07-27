import argparse
import csv
import os
import random


SECONDS_PER_DAY = 86400
RANDOM_RANGE = 2.0


def read_records(input_file):
    """
    Read all records and return:
    - records
    - max_id
    """

    records = []
    max_id = 0

    with open(input_file, "r", newline="") as f:
        reader = csv.reader(f)

        for row in reader:

            record = {
                "id": int(row[0]),
                "timestamp": int(row[1]),
                "value": float(row[2]),
                "property": row[3],
                "plug_id": row[4],
                "household_id": row[5],
                "house_id": row[6],
            }

            max_id = max(max_id, record["id"])

            records.append(record)

    return records, max_id


def write_shifted_day(records, days, output_file):
    """
    Write the shifted original day.
    """

    offset = days * SECONDS_PER_DAY

    with open(output_file, "w", newline="") as f:

        writer = csv.writer(f)

        for r in records:

            writer.writerow([
                r["id"],
                r["timestamp"] + offset,
                f"{r['value']:.3f}",
                r["property"],
                r["plug_id"],
                r["household_id"],
                r["house_id"],
            ])


def write_history(records, max_id, days, output_file):
    """
    Generate historical data.

    Output order:
        DayN
        DayN-1
        ...
        Day1

    DayN là ngày ngay trước file gốc đã dịch timestamp.
    """

    random.seed(42)

    current_id = max_id - 1

    # Giá trị của "ngày sau"
    previous_values = [r["value"] for r in records]

    with open(output_file, "w", newline="") as f:

        writer = csv.writer(f)

        # DayN -> Day1
        for day in range(days, 0, -1):

            # DayN = original + (days-1) ngày
            # ...
            # Day1 = original + 0 ngày
            offset = (day - 1) * SECONDS_PER_DAY

            current_values = []

            for idx, r in enumerate(records):

                value = previous_values[idx] + random.uniform(
                    -RANDOM_RANGE,
                    RANDOM_RANGE,
                )

                value = max(0.0, value)

                current_values.append(value)

                writer.writerow([
                    current_id,
                    r["timestamp"] + offset,
                    f"{value:.3f}",
                    r["property"],
                    r["plug_id"],
                    r["household_id"],
                    r["house_id"],
                ])

                current_id -= 1

            # Ngày tiếp theo sẽ dựa trên ngày vừa sinh
            previous_values = current_values


def main():

    parser = argparse.ArgumentParser(
        description="Generate historical DEBS dataset."
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Input house csv file",
    )

    parser.add_argument(
        "--days",
        type=int,
        required=True,
        help="Number of historical days",
    )

    parser.add_argument(
        "--output-dir",
        required=True,
        help="Output directory",
    )

    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    records, max_id = read_records(args.input)

    basename = os.path.splitext(
        os.path.basename(args.input)
    )[0]

    shifted_file = os.path.join(
        args.output_dir,
        f"{basename}_day_{args.days + 1}.csv",
    )

    history_file = os.path.join(
        args.output_dir,
        f"historical_{basename}_day1_{args.days}.csv",
    )

    write_shifted_day(
        records,
        args.days,
        shifted_file,
    )

    write_history(
        records,
        max_id,
        args.days,
        history_file,
    )

    print("Done.")
    print(f"Shifted day : {shifted_file}")
    print(f"History     : {history_file}")


if __name__ == "__main__":
    main()