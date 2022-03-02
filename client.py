#!/usr/bin/env python3
import os
import socket
import shutil
import uuid
import random
import csv
import io
from timeit import default_timer as timer
import argparse
import hashlib

dir_paths = []
file_paths = []

client_dir = 'client_data/'

server_list = []


round_cnt = 0

#BUFFER_SIZE = 40*1024*1024
BUFFER_SIZE = 4096 #40*1024*1024*1024

# Set arguments
parser = argparse.ArgumentParser(description='Socket client')
parser.add_argument('-m','--bmode', help='Balancer mode: ROUND_ROBIN, HASH, RANDOM', required=True)
args = vars(parser.parse_args())

BALANCER_MODE = args['bmode']

def main():
    # init
    init_storage()
    # Clean folders
    #clean_data()

    # Generate files if they have not been generated
    if not os.listdir(client_dir):
        generate_files('G1',100, 1, 10*1024) # G1 tiny-size files between 1 Byte and 10KB
        generate_files('G2',100, 10*1024, 1024*1024) # G2 small-size files between 10 KB and 1MB
        generate_files('G3',100, 1024*1024, 10*1024*1024) # G3 medium-size files between 1 MB and 10MB
    else:
        import_filepaths()

    groups = ['G1','G2', 'G3']
    print('----- SUBIDA DE ARCHIVOS -----')
    
    for g in groups:
        print('\tGroup: ', g)
        avg_upload_time = upload_files(g)
        print('\tAverage upload time: ', avg_upload_time*1000.0)
        print('')

    export_filepaths()


    print('\n----- DESCARGA DE ARCHIVOS -----')
    
    for g in groups:
        print('\tGroup: ', g)
        avg_download_time = download_files(g)
        print('\tAverage download time: ', avg_download_time*1000.0)
        print('')
    




def load_balancer(file_route):
    global round_cnt

    server = 0
    if BALANCER_MODE == 'ROUND_ROBIN':

        if round_cnt >= len(server_list):
            round_cnt = 0
        
        server = round_cnt
        round_cnt += 1
        
    elif BALANCER_MODE == 'HASH':
        result = hashlib.md5(bytes(file_route,encoding ="utf-8"))
        string_digest = str(result.digest())
        integer_digest = sum(ord(ch) for ch in string_digest)
        server = integer_digest % 3
        
    elif BALANCER_MODE == 'RANDOM':
        server = random.randint(0, len(server_list)-1)
    
    #print('Server: ', server)

    #return hosname and port of selected server
    return server,server_list[server]['host'], int(server_list[server]['port'])




def upload_files(group_name):
    avg_upload_time = 0
    nfiles = 0
    acum_time = 0
    # Upload every single file
    for fpath in file_paths:
        if fpath[0] == group_name:
            # Connect to Server
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                # Call load balancer
                SERVER,HOST,PORT = load_balancer(fpath[1])
                # assign server
                fpath[2] = SERVER

                s.connect((HOST, PORT))

                f = open(fpath[1], 'rb') #io.open(fpath[1], mode="rb") 
                
                binary_data = f.read()
                uri = fpath[1].replace(client_dir,'')
                # get the cursor positioned at end
                f.seek(0, os.SEEK_END)
                content_length = f.tell()

                strbin = binary_data.decode(errors='ignore')

                request = "{METHOD} {URI}\nContent-length: {CONTENT_LENGTH}\n{BODY}".format(METHOD='PUT',URI=uri,CONTENT_LENGTH=content_length,BODY=strbin)
                start = timer()
                try:
                    s.sendall(request.encode())

                except socket.error:
                    # A socket error
                    pass
                except IOError:
                    if e.errno == errno.EPIPE:
                        # EPIPE error
                        response = s.recv(BUFFER_SIZE)
                    else:
                        # Other error
                        pass
                
                end = timer()
                # count time
                acum_time += end - start

                response = s.recv(BUFFER_SIZE)
                
                print('Response upload', response.decode())
                print('')
            
            nfiles += 1

    
    # Calculate average upload time
    if nfiles > 0:
        avg_upload_time = acum_time/nfiles

    return avg_upload_time
    
 
        

def download_files(group_name):
    avg_download_time = 0
    nfiles = 0
    acum_time = 0

    # Download all files individually
    for fpath in file_paths:

        if fpath[0] == group_name:
            # Connect to Server
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                SERVER = fpath[2]
                HOST = server_list[SERVER]['host']
                PORT = int(server_list[SERVER]['port'])

                s.connect((HOST, PORT))

                uri = fpath[1].replace(client_dir,'')

                request = "{METHOD} {URI}\n{BODY}".format(METHOD='GET',URI=uri,BODY='')
                s.sendall(request.encode())
                start = timer()

                bbuffer = b''
                while True:
                    bytes_response = s.recv(BUFFER_SIZE)
                    # write to the file the bytes we just received
                    bbuffer += bytes_response

                    if len(bytes_response) < BUFFER_SIZE:
                        # nothing is received
                        # file transmitting is done
                        break
                
                
                end = timer()
                # count time
                acum_time += end - start

                response = bbuffer

                #print('Response download', response.decode())
                #print('')
            
            nfiles += 1

    if nfiles > 0:    
        # Calculate average download time
        avg_download_time = acum_time/nfiles

    return avg_download_time
        

    

def generate_files(group_name,num_files, min_size, max_size):
    global dir_paths,file_paths

    # mk random dir
    dirname = group_name+'_'+str(uuid.uuid1())

    dir_path = os.path.join(client_dir,dirname)

    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    dir_paths.append(dir_path)
    
    # Generate <num_files> random files
    for i in range(num_files):

        # Generate file
        filename = str(uuid.uuid1())
        file_path = os.path.join(dir_path, filename)
        size = random.randint(min_size, max_size)

        # with open(file_path, 'wb') as fout:
        #     # Generate random binary string
        #     fout.write(os.urandom(size))

        with io.open(file_path,'w',encoding='utf8') as f:
            f.write('0' * size)

        file_paths.append([group_name,file_path,None])


def import_filepaths():
    global file_paths
    # Import server list
    with open('filepaths.csv', mode='r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            else:
                file_paths.append([row[0],row[1],row[2]])
    
def export_filepaths():
    # open the file in the write mode
    f = open('filepaths.csv', 'w')

    # create the csv writer
    writer = csv.writer(f)

    for fpath in file_paths:
        # write a row to the csv file
        writer.writerow([fpath[0],fpath[1],str(fpath[2])])
    
    # close the file
    f.close()

def init_storage():
    global server_list
    # create storage dir if not exists
    if not os.path.exists(client_dir):
        os.makedirs(client_dir)

    # Import server list
    with open('serverlist.csv', mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            
            server_list.append(row)
            
            line_count += 1

def clean_data():
    if not os.path.exists(client_dir):
        os.makedirs(client_dir)
    
    for filename in os.listdir(client_dir):
        file_path = os.path.join(client_dir, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))



if __name__ == "__main__":
    main()