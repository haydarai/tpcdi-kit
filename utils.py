import operator
import tempfile
import os
import string
import random
from contextlib import ExitStack 
from heapq import merge


class CSV_Transformer():
    """
    TODO: Make an abstract class, and this should extend the abstract class
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

def external_sort(input_file, transformer, col_idx, max_chunk_size=50):
    """
    Sort file based on col_idx outside main memory. 
    Args:
        input_file (str): Path to the input file.
        transformer (obj): Instance of a row-to-list transformer class.
        col_idx (int): Index of the list column used for sorting.
        max_chunk_size (int): Maximum number of lines contained in a chunk file.
        on_finished (fun): Function to be executed with the outputfile as parameter when the sorting is finished.

    WARNING: This will perform inplace operation, sorted version of the file will be written in the input file
    """

    chunk = []
    chunk_names = []
    counter = 0
    with open(input_file) as f, tempfile.TemporaryDirectory() as tmpdirname:
        # Split input to multiple files
        for line in f:
            chunk.append(transformer.transofrm(line))
            if (len(chunk)==max_chunk_size or (not line.endswith('\n'))):
 
                chunk_name = 'chunk_{}.chk'.format(counter)
                chunk = sorted(chunk, key=lambda row: row[col_idx], reverse=False)
                chunk_names.append(os.path.join(tmpdirname, chunk_name))
                with open(os.path.join(tmpdirname, chunk_name), 'w') as split_file:
                    for chunk_line in chunk:
                        original_line = transformer.inverse_transform(chunk_line)
                        if not original_line.endswith('\n'):
                            original_line=original_line+'\n'
                        split_file.write(original_line)
                chunk = []
                counter +=1  
        if len(chunk)>0:
            chunk_name = 'chunk_{}.chk'.format(counter)
            chunk = sorted(chunk, key=lambda row: row[col_idx], reverse=False)
            chunk_names.append(os.path.join(tmpdirname, chunk_name))
            with open(os.path.join(tmpdirname, chunk_name), 'w') as split_file:
                for chunk_line in chunk:
                    original_line = transformer.inverse_transform(chunk_line)
                    if not original_line.endswith('\n'):
                        original_line=original_line+'\n'
                    split_file.write(original_line)              
        # Merge        
        # Credit: https://stackoverflow.com/questions/23450145/sort-a-big-file-with-python-heapq-merge
        with ExitStack() as stack, open(input_file, 'w') as output_file:
            files = [stack.enter_context(open(chunk)) for chunk in chunk_names]
            output_file.writelines(merge(*files, key=lambda row: transformer.transofrm(row)[col_idx]))
        return input_file

def sort_merge_join(left, right, left_on, right_on, left_trf, right_trf):
    sorted_left = external_sort(input_file=left,
                                transformer = left_trf, 
                                col_idx=left_on)

    # Sort right, get iterator of sorted_right
    sorted_right = external_sort(input_file=right,
                                transformer = right_trf, 
                                col_idx=right_on)

    with open (sorted_left) as sorted_left_file, open(sorted_right) as sorted_right_file:  
        curr_l_value = left_trf.transofrm(next(sorted_left_file))
        curr_r_value = right_trf.transofrm(next(sorted_right_file))
        try:
            while(True):
                if curr_l_value[left_on]<curr_r_value[right_on]:
                    curr_l_value = left_trf.transofrm(next(sorted_left_file))
                elif curr_l_value[left_on]>curr_r_value[right_on]:
                    curr_r_value = right_trf.transofrm(next(sorted_right_file))
                elif curr_l_value[left_on] == curr_r_value[right_on]:
                    yield (curr_l_value + curr_l_value)
                    curr_l_value = left_trf.transofrm(next(sorted_left_file))
        except StopIteration:
            pass

  