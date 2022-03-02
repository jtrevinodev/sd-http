# INTEGRANTES:
    - José Treviño Olvera
    - Brucelee Campos Alvarado

# CORRER EJEMPLO
1. Correr los siguientes comandos en terminales distintas:
    - Servidor 1:
        python3 server.py -n S1 -p 12345
    - Servidor 2:
        python3 server.py -n S2 -p 12346
    - Servidor 3:
        python3 server.py -n S3 -p 12347

    NOTA: Editar archivo 'serverlist.csv' si desea cambiar el host y puerto de los servidores.

2. Correr cliente en terminal.
    - Cliente y balanceador:
        python3 client.py -m 'ROUND_ROBIN' > output.txt
