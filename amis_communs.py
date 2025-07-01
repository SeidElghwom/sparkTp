from pyspark import SparkContext

sc = SparkContext("local", "AmisCommuns")

# Charger les données
lines = sc.textFile("C:\Users\desk\Desktop\M\Spark\soc-LiveJournal1Adj.txt")

# Mapper les lignes en (user_id, nom, [amis])
def mapper(line):
    parts = line.strip().split()
    if len(parts) < 3:
        return None
    user_id = int(parts[0])
    name = parts[1]
    friends = list(map(int, parts[2].split(',')))
    return (user_id, name, friends)

mapped = lines.map(mapper).filter(lambda x: x is not None)

# Dictionnaire {id: nom} pour retrouver les noms
names_dict = mapped.map(lambda x: (x[0], x[1])).collectAsMap()

# Générer les paires d'amis (min, maax) avec liste d'amis
def generate_pairs(user_id, friends):
    return [ (tuple(sorted((user_id, friend))), set(friends)) for friend in friends ]

pairs = mapped.flatMap(lambda x: generate_pairs(x[0], x[2]))

# Trouver les amis communs entre chaque paire
common_friends = pairs.reduceByKey(lambda x, y: x & y)

# Filtrer la paire (1, 2)
target_pair = (1, 2)
filtered = common_friends.filter(lambda x: x[0] == target_pair)

# Afficher le résultat au format demandé
for ((u1, u2), amis_communs) in filtered.collect():
    nom1 = names_dict.get(u1, str(u1))
    nom2 = names_dict.get(u2, str(u2))
    print(f"{u1}<{nom1}>{u2}<{nom2}>{sorted(amis_communs)}")
