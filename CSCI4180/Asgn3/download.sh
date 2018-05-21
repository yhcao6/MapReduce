# rm mydedup.index

javac -cp .:lib/* LocalStorage.java Index.java MyDedup.java && 

# java -cp .:./lib/* MyDedup download ./1.png local && mv 1.png.download 2.png
java -cp .:./lib/* MyDedup download ./1.txt local && mv 1.txt.download 2.txt

rm *.class
