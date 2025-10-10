import csv
import argparse

class LogEntry:
    def __init__(self, process_name, process_id, cpu_id, timestamp, runqueue_cpu, field, function, access_type):
        self.process_name = process_name
        self.process_id = process_id
        self.cpu_id = cpu_id
        self.timestamp = float(timestamp)
        self.runqueue_cpu = runqueue_cpu
        self.field = field
        self.function = function
        self.access_type = access_type
        self.data_type = None
        self.attributes = None
        self.offset = None
        self.size = None

    def to_dict(self):
        return {
            'Process_Name': self.process_name,
            'Process_ID': self.process_id,
            'CPU_ID': self.cpu_id,
            'Timestamp': self.timestamp,
            'Runqueue_CPU': self.runqueue_cpu,
            'Field': self.field,
            'Function': self.function,
            'Access_Type': self.access_type,
            'Data_Type': self.data_type,
            'Attributes': self.attributes,
            'Offset': self.offset,
            'Size (Bytes)': self.size
        }

class LogDAO:
    def __init__(self):
        self.entries = []

    def add_entry(self, log_entry):
        self.entries.append(log_entry)

    def save_to_csv(self, csv_file):
        csv_fields = [
            'Process_Name', 'Process_ID', 'CPU_ID', 'Timestamp', 'Runqueue_CPU', 'Field',
            'Function', 'Access_Type', 'Data_Type', 'Attributes', 'Offset', 'Size (Bytes)'
        ]
        with open(csv_file, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=csv_fields)
            writer.writeheader()
            for entry in self.entries:
                writer.writerow(entry.to_dict())

    def filter_cross_cpu_access(self):
        self.entries = [entry for entry in self.entries if int(entry.cpu_id) == int(entry.runqueue_cpu)]

def parse_log_line(line):
    parts = line.split()
    process_name = parts[0]
    process_id = parts[1]
    cpu_id = parts[2][1:-1]
    timestamp = parts[3][:-1]
    runqueue_part = parts[6]
    runqueue_cpu = runqueue_part.split('[')[1].split(']')[0]
    field = runqueue_part.split('->')[1]
    function = parts[8]
    access_type = parts[9][1:-1]
    return LogEntry(process_name, process_id, cpu_id, timestamp, runqueue_cpu, field, function, access_type)

def parse_log_file(file_path):
    log_dao = LogDAO()
    with open(file_path, 'r') as f:
        for line in f:
            entry = parse_log_line(line)
            log_dao.add_entry(entry)
    return log_dao

def parse_pahole_data(file_path):
    parsed_data = {}
    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()
            if "/*" in line and 'XXX' not in line:
                parts = line.split("/*")
                field_info = parts[0].strip()
                if not field_info:
                    continue
                tokens = field_info.split("__attribute__")
                data_type = " ".join(tokens[0].strip().split(" ")[:-1]).strip()
                field_name = tokens[0].strip().split(" ")[-1].replace(";", "")
                attributes = tokens[1].replace(";", "") if len(tokens) == 2 else ''
                try:
                    size_info = (' '.join(parts[1].split()).replace("*/", "")).split(" ")
                    parsed_data[field_name] = [data_type, attributes, size_info[0], size_info[1]]
                except (ValueError, AttributeError):
                    continue
    return parsed_data

def merge_data(log_dao, pahole_data):
    for entry in log_dao.entries:
        if entry.field in pahole_data:
            entry.data_type, entry.attributes, entry.offset, entry.size = pahole_data[entry.field]

def main():
    parser = argparse.ArgumentParser(description='Parse log and pahole data files and export combined data to CSV.')
    parser.add_argument('log_file', help='Path to the log file to be parsed')
    parser.add_argument('pahole_file', help='Path to the pahole data file to be parsed')
    parser.add_argument('csv_file', help='Path to save the combined CSV file')
    parser.add_argument('--exclude_cross_cpu', action='store_true', 
                        help='Exclude cross-CPU access entries from the final CSV')

    args = parser.parse_args()

    log_dao = parse_log_file(args.log_file)
    pahole_data = parse_pahole_data(args.pahole_file)

    merge_data(log_dao, pahole_data)

    if args.exclude_cross_cpu:
        log_dao.filter_cross_cpu_access()

    log_dao.save_to_csv(args.csv_file)

    print(f"Data successfully parsed, merged, and saved to {args.csv_file}")

if __name__ == '__main__':
    main()
