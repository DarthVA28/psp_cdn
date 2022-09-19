def makeChunks(text, width):
    chunks = []
    curr_chunk = ""
    for v in text:
        curr_chunk += v
        if len(curr_chunk) == width:
            chunks.append(curr_chunk)
            curr_chunk = ""
    if (len(curr_chunk) != 0):
        chunks.append(curr_chunk)
    return chunks

def processFile(filename):
    # Open the file 
    server_file = open(filename, "r") 
    
    # Read the data into a string
    file_data = server_file.read()

    # Divide the file into nclient chunks
    chunk_size = 1024 # 1 kB sized chunks
    chunks = makeChunks(file_data, chunk_size)
    return chunks 

def getCIdx(message):
    left_idx = 4
    right_idx = 0
    i = 0
    while (i < len(message)):
        if (message[i] == "$"):
            right_idx = i
            break
        else:
            i += 1
    return int(message[left_idx:right_idx])