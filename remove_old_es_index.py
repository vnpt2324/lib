import string
import os
import glob
import re
from operator import itemgetter
import shutil
import argparse

def get_index_name(path_file):
    file = open(path_file, "r")
    data = file.read()
    file.close()


    start_index = data.find("logstash-")
    end_index = start_index
    for i in range(start_index, len(data)):
        text = data[i]
        if not text in string.printable:
            end_index = i
            break

    index_name = data[start_index:end_index]

    print(path_file, index_name)
    return index_name

def get_state_file(index_folder):
    path = os.path.join(index_folder, "_state", "state*.st")
    file_list = glob.glob(path)
    for file_name in file_list:
        return get_index_name(file_name)

def get_date_time(index_name):
    try:
        m = re.search('(\d+\.\d+\.\d+)', index_name)
        if m:
            found = m.group(1)
            return found
    except Exception as e:
        print(e)
    return ""


def run(path_folder, number_file_remove):
    # es_index_path = "/esdata/elasticsearch/nodes/0/indices"
    index_folder_list = glob.glob(os.path.join(path_folder, "*"))
    mapping_folder = []
    for index_folder in index_folder_list:
        index_name = get_state_file(index_folder)
        date_index = get_date_time(index_name)
        if not date_index:
            continue
        mapping_folder.append(
            {
                "folder": index_folder,
                "date": date_index
            }
        )

    mapping_folder = sorted(mapping_folder, key=itemgetter('date'))
    # for a in mapping_folder:
    #     print a

    for i in range(0, number_file_remove):
        folder = mapping_folder[i]
        print("removing: folder_name=", folder["folder"], " , date=", folder['date'])
        try:
            shutil.rmtree(folder["folder"])
        except Exception as e:
            print(e)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Remove directly shard from raw shard file. Tool will remove <rm_files> oldest indexs in  <es_path> folder, with index pattern name is: *%Y.%M.%d*, ex: logstash-2018.09.26-http. NOTE: Before run tool, please stop elasticsearch service. \nTool run as python 2.7')
    parser.add_argument("-es_path", '--es_path', help="Path of indices ES. Example: '/esdata/elasticsearch/nodes/0/indices'")
    parser.add_argument("-rm_files", '--rm_files', type=int, default=0, help="Number of file need remove. Example: 10")
    args = parser.parse_args()

    run(args.es_path, args.rm_files)









