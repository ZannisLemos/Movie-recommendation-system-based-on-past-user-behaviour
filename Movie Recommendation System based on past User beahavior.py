# -*- coding: utf-8 -*-

import sys
from pyspark import SparkConf, SparkContext
from math import sqrt


def loadMovieNames():
    movieNames = {}
    with open(r"C:\data_project") as f:
        for line in f:
            fields = line.split("::")
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movieNames

def zeugaria_tainiwn_vathmologiwn((user, vathmologies)):
    (tainia1, vathmologies1) = vathmologies[0]
    (tainia2, vathmologies2) = vathmologies[1]
    return ((tainia1, tainia2), (vathmologies1, vathmologies2))

def filtrarisma_tainiwn( (userID, vathmologies) ):
    (tainia1, rating1) = vathmologies[0]
    (tainia2, rating2) = vathmologies[1]
    return tainia1 < tainia2
    
def ypologismos_omoiotitas(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)


conf = SparkConf().setMaster("local[*]").setAppName("movies")
sc = SparkContext(conf = conf)

print "\nLoading movie names..."
nameDict = loadMovieNames()

dataset = sc.textFile("file:///data_project/10m/ratings.dat")

# Key-value ζευγαρια                                                   UserID        MovieID     Rating  
vathmologies = dataset.map(lambda x: x.split("::")).map(lambda x: (int(x[0]), (int(x[1]), float(x[2]))))


# Self-join για να βρούμε όλα τα πιθανά ζευγάρια ταινιών.
vathmologies_Partitioned = vathmologies.partitionBy(1000)
self_join = vathmologies_Partitioned.join(vathmologies_Partitioned)

# Ως εδω το RDD μας είναι userID => ((movieID, rating), (movieID, rating))

# Φιλτράρουμε τα διπλά ζευγάρια
monadika_zeugaria = self_join.filter(filtrarisma_tainiwn)

# Τώρα το key είναι (tainia1, tainia2) τα ζευγάρια των ταινιών.
zeugaria_tainiwn = monadika_zeugaria.map(zeugaria_tainiwn_vathmologiwn).partitionBy(1000)

# Τώρα έχουμε (ταινία1, ταινία2) => (βαθμολογία1, βαθμολογία2)
# μαζεύουμε όλες τις βαθμολογίες για κάθε ζευγάρι ταινιών
sullogi_vathmologiwn = zeugaria_tainiwn.groupByKey()

# Τώρα έχουμε (ταινία1, ταινία2) => (βαθμολογία1, βαθμολογία2), (βαθμολογία1, βαθμολογία2)...
# υπολογισμος ομοιότητας
skor_omoiotitas = sullogi_vathmologiwn.mapValues(ypologismos_omoiotitas).persist()

# ταξινόμηση
skor_omoiotitas.sortByKey()
skor_omoiotitas.saveAsTextFile("movie-sim")

# επιστρέφει αποτελέσματα με υψηλο σκορ ομοιότητας.
if (len(sys.argv) > 1):

    scoreThreshold = 0.97
    coOccurenceThreshold = 10000

    movieID = int(sys.argv[1])

    # φιλτράρουμε τις ταινίες που θα μας επιστρέψει ανάλογα με το threshold που του έχουμε δώσει
    filteredResults = skor_omoiotitas.filter(lambda((pair,sim)): \
        (pair[0] == movieID or pair[1] == movieID) \
        and sim[0] > scoreThreshold and sim[1] > coOccurenceThreshold)

    # Ταξινόμηση βάση αποτελέσματος
    results = filteredResults.map(lambda((pair,sim)): (sim, pair)).sortByKey(ascending = False).take(10)

    print "Top 10 similar movies for " + nameDict[movieID]
    for result in results:
        (sim, pair) = result
        similarMovieID = pair[0]
        if (similarMovieID == movieID):
            similarMovieID = pair[1]
        print nameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1])