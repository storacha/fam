fam:
	go build -o ./fam ./cmd

.PHONY: clean-fam

clean-fam:
	rm -f ./fam
