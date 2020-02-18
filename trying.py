import time
import bz2file
def blocks(files, size=65536):
    while True:
        b = files.read(size)
        if not b: break
        yield b

start_time = time.time()

# file = 'C:/Users/Ion/IVT/OSM_data/liechtenstein-latest.osm.bz2'

with open(file, "r",encoding="utf-8",errors='ignore') as f:
    # for line in f:
    #     print(line)
    print (sum(bl.count("\n") for bl in blocks(f)))
    # print(sum(bl.count(b"<node ") for bl in blocks(f)))
    # print(sum(bl.count("<way ") for bl in blocks(f)))

end_time = time.time()
print(end_time - start_time)

start_time = time.time()
file = 'C:/Users/Ion/IVT/OSM_data/switzerland-latest.osm.bz2'
node_count = 0
way_count = 0
with bz2file.open(str(file)) as f:
    for line in f:
        if b"<node " in line:
            node_count += 1
        if b"<way " in line:
            way_count +=1
print(node_count, way_count)
end_time = time.time()
print(end_time - start_time)

