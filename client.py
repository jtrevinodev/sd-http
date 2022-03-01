#!/usr/bin/env python3
import os
import socket
import shutil
import uuid
import random
import csv

dir_paths = []
file_paths = []

client_dir = 'client_data/'

server_list = []

BALANCER_MODE = 'RANDOM'
round_cnt = 0

BUFFER_SIZE = 40*1024*1024

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

    print('----- SUBIDA DE ARCHIVOS -----')
    upload_files()

    print('\n----- DESCARGA DE ARCHIVOS -----')
    download_files()
    




def load_balancer():
    global round_cnt

    server = 0
    if BALANCER_MODE == 'ROUND_ROBIN':

        if round_cnt >= len(server_list):
            round_cnt = 0
        
        server = round_cnt
        round_cnt += 1
        
    elif BALANCER_MODE == 'HASH':
        pass
    elif BALANCER_MODE == 'RANDOM':
        server = random.randint(0, len(server_list)-1)
    
    print('Server: ', server)

    #return hosname and port of selected server
    return server,server_list[server]['host'], int(server_list[server]['port'])




def upload_files():
    # Upload every single file
    for fpath in file_paths:
        # Connect to Server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # Call load balancer
            SERVER,HOST,PORT = load_balancer()
            # assign server
            fpath[1] = SERVER

            s.connect((HOST, PORT))

            f = open(fpath[0], 'rb')
            binary_data = f.read()
            uri = fpath[0].replace(client_dir,'')
            # get the cursor positioned at end
            f.seek(0, os.SEEK_END)
            content_length = f.tell()

            request = "{METHOD} {URI}\nContent-length: {CONTENT_LENGTH}\n{BODY}".format(METHOD='PUT',URI=uri,CONTENT_LENGTH=content_length,BODY=binary_data)
            try:
                s.sendall(request.encode())
            except socket.error:
                # A socket error
                pass
            except IOError:
                if e.errno == errno.EPIPE:
                    # EPIPE error
                    response = s.recv(BUFFER_SIZE)
                
                    print('Response upload2', response.decode())
                    print('')
                else:
                    # Other error
                    pass
            response = s.recv(BUFFER_SIZE)
            
            print('Response upload', response.decode())
            print('')

    export_filepaths()
                
            
            
            
        

def download_files():
    # Download all files individually
    for fpath in file_paths:
        # Connect to Server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            SERVER = fpath[1]
            HOST = server_list[SERVER]['host']
            PORT = int(server_list[SERVER]['port'])

            s.connect((HOST, PORT))

            uri = fpath[0].replace(client_dir,'')

            request = "{METHOD} {URI}\n{BODY}".format(METHOD='GET',URI=uri,BODY='')
            s.sendall(request.encode())
            response = s.recv(BUFFER_SIZE)

            print('Response download', response.decode())
            print('')
        

    

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

        with open(file_path, 'wb') as fout:
            # Generate random binary string
            fout.write(os.urandom(size))

        file_paths.append([file_path,None])


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
                file_paths.append([row[0],row[1]])
    
def export_filepaths():
    # open the file in the write mode
    f = open('filepaths.csv', 'w')

    # create the csv writer
    writer = csv.writer(f)

    for fpath in file_paths:
        # write a row to the csv file
        writer.writerow([fpath[0],str(fpath[1])])
    
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