.PHONY: all clean
all: bin/mesh-supervisor
bin/mesh-supervisor: src/mesh_supervisor.py src/worker.py
	mkdir -p bin
	cp src/mesh_supervisor.py bin/mesh-supervisor
	chmod 0755 bin/mesh-supervisor src/worker.py
clean:
	rm -rf bin
