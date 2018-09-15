data:
	mkdir data
	kaggle datasets download -d zynicide/wine-reviews -f winemag-data-130k-v2.csv -p ./data/ 
	unzip data/winemag-data-130k-v2.csv -d data/
	rm data/winemag-data-130k-v2.csv.zip
