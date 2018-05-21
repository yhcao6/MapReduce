file = ["shakespeare-basket1", "shakespeare_basket2"]
for f in file:
    with open(f, 'r') as fi_in, open('preprocess_'+f, 'w') as fi_out:
        for line in fi_in:
            seen = set()
            l = list()
            for word in line.split(" "):
                if word not in seen:
                    seen.add(word)
                    l.append(word)
            fi_out.write(" ".join(l))
