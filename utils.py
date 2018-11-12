import operator
import tempfile
import os
import string
import random
from contextlib import ExitStack 
from heapq import merge

class CSV_Transformer():
    """
    Transform a row in a csv flat file to a list
    Attributes:
        delimiter (str): Character used to limit a field entries to other fields.
    """
    def __init__(self, delimiter):
        self.delimiter = delimiter
    def transofrm(self, line):
        return line.split(self.delimiter)
    def inverse_transform(self, transformed):
        return self.delimiter.join(transformed)

def external_sort(input_file, transformer, col_idx, max_chunk_size, on_finished):
    """
    Sort file based on col_idx outside main memory. 
    Args:
        input_file (str): Path to the input file.
        transformer (obj): Instance of a row-to-list transformer class.
        col_idx (int): Index of the list column used for sorting.
        max_chunk_size (int): Maximum number of lines contained in a chunk file.
        on_finished (fun): Function to be executed with the outputfile as parameter when the sorting is finished.
    """
    
    chunk = []
    chunk_names = []
    counter = 0
    with open(input_file) as f, tempfile.TemporaryDirectory() as tmpdirname:
        # Split input to multiple files
        for line in f:
            if len(chunk)<max_chunk_size:
                chunk.append(transformer.transofrm(line))
            else:
                chunk_name = 'chunk_{}.chk'.format(counter)
                chunk = sorted(chunk, key=lambda row: row[col_idx], reverse=False)
                chunk_names.append(os.path.join(tmpdirname, chunk_name))
                with open(os.path.join(tmpdirname, chunk_name), 'w') as split_file:
                    for line in chunk:
                        split_file.write(transformer.inverse_transform(line))
                counter +=1
                chunk = []
        # Merge        
        # Credit: https://stackoverflow.com/questions/23450145/sort-a-big-file-with-python-heapq-merge
        output_path = os.path.join(tmpdirname, input_file.split('/')[-1])
        with ExitStack() as stack, open(output_path, 'w') as output_file:
            files = [stack.enter_context(open(chunk)) for chunk in chunk_names]
            output_file.writelines(merge(*files, key=lambda row: transformer.transofrm(row)[col_idx]))

            # Do something with the sorted file
            on_finished(output_path)

def dummyOnFinish(sorted_file):
    pass

# Usage Example
if __name__ == "__main__":
    pipe_delimited_transformer = CSV_Transformer(delimiter="|")
    external_sort(input_file='staging/5/Batch1/Industry.txt',transformer = pipe_delimited_transformer, col_idx=1, max_chunk_size=2, on_finished=dummyOnFinish)