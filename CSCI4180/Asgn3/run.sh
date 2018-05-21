# rm mydedup.index

javac -cp .:lib/* S3Storage.java Index.java MyDedup.java

# java -cp .:./lib/* MyDedup upload 4 13 1000 10 1.txt s3
# java -cp .:./lib/* MyDedup upload 400 512 1000 257 2.png s3

# delete
java -cp .:./lib/* MyDedup delete 1.txt s3
# java -cp .:./lib/* MyDedup delete 1.png s3

# download
# java -cp .:./lib/* MyDedup download 1.txt s3 && mv 1.txt.download 2.txt
# java -cp .:./lib/* MyDedup download 1.png s3 && mv 1.png.download 2.png

rm *.class
