from sklearn.cluster import KMeans
import sys
import pandas as pd
import scipy.stats
import matplotlib.pyplot as plt

filename = 'data/yearly_phrases_docs_frequency.tsv'
cluster_num = 10

'''
mag = pd.read_csv('mag_fos.txt', sep="\t", header=None)
mag.columns = ['id', 'rank', 'normalized name', 'display name', 'main type', 'level', 'paper count', 'citation count', 'created date']
mag = mag.loc[mag['paper count'] > 200]
'''


acm_terms = []
with open('ccs.xml') as f:
    for line in f:
        if line.startswith('<skos:prefLabel xml:lang="en">'):
            term = line.replace('<skos:prefLabel xml:lang="en">', '').replace('</skos:prefLabel>', '')
            acm_terms.append(term.lower().strip())
        if line.startswith('<skos:altLabel xml:lang="en">'):
            term = line.replace('<skos:altLabel xml:lang="en">', '').replace('</skos:altLabel>', '')
            acm_terms.append(term.lower().strip())


'''
algorithms = []
with open('algorithms.txt') as f:
    for line in f:
        algorithms.append(line.lower().strip())
'''

df = pd.read_csv(filename, sep="\t")
#df = df.loc[df['phrase'].isin(mag['normalized name'].tolist())]
df = df.loc[df['phrase'].isin(acm_terms)]

time_series = df.drop(columns=['phrase', 'total_occurrences', 'total_documents']).values

norm_time_series = scipy.stats.zscore(time_series, axis=1)

clusters = KMeans(n_clusters=cluster_num)
pred = clusters.fit_predict(norm_time_series)
df['cluster'] = pred

# record centroids of clusters

centers = clusters.cluster_centers_
c = 0
f = open('result/centers.txt', 'w')
for center in centers:
    f.write(str(c) + '\t' + str(center) + '\n')
    c += 1
f.close()

# visualize clusters

x = [2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017]

c = 0
with open('result/cluster_phrase.txt', 'w') as w:
    for center in centers:
        plt.plot(x, center, label = str(c))
        plt.legend()

        output = str(c) + '\t'

        members = df[df['cluster'] == c]
        for i, row in members.iterrows():
            output += str(row['phrase']) + '\t'

        w.write(output.strip() + '\n')
        c += 1

plt.show()
