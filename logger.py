import os

def sort_log_file(input_file, output_file):
    with open(input_file, 'r') as file:
        lines = file.readlines()

    # 按照第一列的时间戳排序
    sorted_lines = sorted(lines, key=lambda line: int(line.split()[0]))

    with open(output_file, 'w') as file:
        file.writelines(sorted_lines)

if __name__ == "__main__":
    input_file = '/home/ayanokouji/code/Distrubute/6.5840/src/raft1/log.txt'
    output_file = '/home/ayanokouji/code/Distrubute/6.5840/src/raft1/sorted_log.txt'
    
    sort_log_file(input_file, output_file)
    print(f"Sorted log file has been written to {output_file}")