# python3 server.py -n S1 -p 12345
import os
import socket
import argparse
import io
import shutil

# Set arguments
parser = argparse.ArgumentParser(description='Socket Server')
parser.add_argument('-n','--hostname', help='Hostname', required=True)
parser.add_argument('-p','--port', help='Port number', required=True)
args = vars(parser.parse_args())

BUFFER_SIZE = 40*1024*1024

def main():
    init_storage()

    #clean_data()

    # Socket
    s = socket.socket()
    print("Socket successfully created")

    # Socket port
    port = int(args['port'])

    # Bind the socket port
    s.bind(('', port))		
    print("socket binded to %s" %(port))

    # Put the socket into listening mode
    s.listen(0) #s.listen(5)
    print("socket is listening")		

    # Accept connections until interrumption
    while True:

        # Establish connection with client.
        c, addr = s.accept()	
        print('Got connection from', addr )

        request = c.recv(BUFFER_SIZE)

        #print('Data recieved: ', request)

        response = read_request(request)

        # Send response to the client
        c.send(response.encode())

        # Close the connection with the client
        c.close()

        # Breaking once connection closed
        #break


def read_request(request):
    # Request attributes
    method = ''
    uri = ''
    content_length = 0
    body = []

    content_flag = False

    try:
        # Read bytes one by one
        reqbytes = io.BytesIO(request)
        byte = reqbytes.read(1)
        init_byte = 0
        end_byte = 0
        byte_cnt = 0
        empty_line = False

        # Read until final byte
        while byte:
            byte_decoded = byte.decode()

            # Read METHOD
            if not method and byte_decoded == ' ':
                end_byte = byte_cnt
                method = request[init_byte:end_byte].decode()
            
            # Read URI
            if method and not uri and byte_decoded == '\n':
                init_byte = end_byte + 1
                end_byte = byte_cnt
                uri = request[init_byte:end_byte].decode()
            
            if method == 'PUT':
                # Read CONTENT-LENGTH
                if method and uri and not content_length and not content_flag and byte_decoded == ' ':
                    init_byte = byte_cnt + 1
                    content_flag = True
                
                if method and uri and not content_length and content_flag and byte_decoded == '\n':
                    end_byte = byte_cnt
                    content_length = int(request[init_byte:end_byte].decode())
            
            elif method == 'GET':
                content_length = 1
            
            # Hop \n empty line
            if content_length and not empty_line and byte_decoded == '\n':
                empty_line = True
                init_byte = byte_cnt + 1

                break

            #byte=false at end of file
            #print(byte)
            byte = reqbytes.read(1)

            byte_cnt += 1

        # Get body content using content_length
        content_length = content_length
        end_byte = init_byte + content_length
        body = request[init_byte:end_byte]

        print('METHOD:', method)
        print('URI:', uri)
        print('Content-length:',content_length)
        print('Body:',body)

        
        uri_elements = uri.split('/')

        file_path = ''
        file_name = ''
        file_dir = os.path.join(server_dir, uri_elements[0])
        
        if len(uri_elements) > 1:
            file_path = os.path.join(server_dir, uri)
            file_name = uri_elements[1]
        
        # Evaluate method
        if method == 'PUT':
            # Create directory if not exists
            if not os.path.exists(file_dir):
                os.makedirs(file_dir)
            
            # Write file into directory
            with open(file_path, 'wb') as fout:
                fout.write(body)

            status = '200 OK'
            response = "{STATUS}\n{BODY}".format(STATUS=status,BODY='')
            
        elif method == 'GET':
            # Single file
            if file_name:
                # Read file from server directory
                try:
                    f = open(file_path, 'rb')
                    binary_data = f.read()
                    f.seek(0, os.SEEK_END)
                    content_length = f.tell()

                    # Return response
                    status = '200 OK'
                    response = "{STATUS}\nContent-length: {CONTENT_LENGTH}\n{BODY}".format(STATUS=status,CONTENT_LENGTH=content_length,BODY=binary_data)
                    return response
                except:
                    status = '404 NOT FOUND'
                    response = "{STATUS}\n{BODY}".format(STATUS=status,BODY='')
                    return response

                
            else:
                # Full directory
                pass

    except:
        status = '5O0 ERROR'
        response = "{STATUS}\n{BODY}".format(STATUS=status,BODY='')

    

    return response


def init_storage():
    global server_dir

    # Hostname
    hostname = args['hostname']

    # Create server storage folder
    server_dir = 'server_'+hostname.replace(' ','')
    if not os.path.exists(server_dir):
        os.makedirs(server_dir)


def clean_data():
    if not os.path.exists(server_dir):
        os.makedirs(server_dir)
    
    for filename in os.listdir(server_dir):
        file_path = os.path.join(server_dir, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))



if __name__ == "__main__":
    main()
