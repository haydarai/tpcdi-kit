import operator
import tempfile
import os
import string
import random
from contextlib import ExitStack 
from heapq import merge

class CSV_Transformer():
    @staticmethod
    def transofrm(line,delimiter):
        return line.split(delimiter)
    
    @staticmethod
    def inverse_transform(transformed, delimiter):
        return delimiter.join(transformed)

def external_sort(input_file, col_idx, max_chunk_size, onFinished):
    """

    """
    
    chunk = []
    chunk_names = []
    counter = 0
    with open(input_file) as f, tempfile.TemporaryDirectory() as tmpdirname:
        # Split input to multiple files
        for line in f:
            if len(chunk)<max_chunk_size:
                chunk.append(CSV_Transformer.transofrm(line, "|"))
            else:
                chunk_name = 'chunk_{}.chk'.format(counter)
                chunk = sorted(chunk, key=lambda row: row[col_idx], reverse=False)
                chunk_names.append(os.path.join(tmpdirname, chunk_name))
                with open(os.path.join(tmpdirname, chunk_name), 'w') as split_file:
                    for line in chunk:
                        split_file.write(CSV_Transformer.inverse_transform(line, "|"))
                counter +=1
                chunk = []
        # Merge        
        # Credit: https://stackoverflow.com/questions/23450145/sort-a-big-file-with-python-heapq-merge
        output_path = os.path.join(tmpdirname, input_file.split('/')[-1])
        with ExitStack() as stack, open("output_path", 'w') as output_file:
            files = [stack.enter_context(open(chunk)) for chunk in chunk_names]
            output_file.writelines(merge(*files, key=lambda row: CSV_Transformer.transofrm(row, "|",)[col_idx]))

            # Do something with the sorted file
            onFinished(output_path)

def dummyOnFinish(sorted_file):
    pass

# Usage Example
if __name__ == "__main__":
    external_sort(input_file='staging/5/Batch1/Industry.txt',col_idx=2, max_chunk_size=2, onFinished=dummyOnFinish)