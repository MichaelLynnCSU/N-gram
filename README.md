# Overview
Generates the following unigram profiles (from a 1G dataset of Wikipedia articles) using MapReduce:

Profile 1
A list of unigrams that occurred at least once in the entire corpus. The unigrams are sorted in (ascending) alphabetical order. No duplicates.

Profile 2
A list of unigrams and their frequencies within the target article. This profile is generated per article. The resulting list is grouped by the Document ID, and sorted (in descending order) on the frequency of the unigram within the article.

Profile 3
A list of unigrams and their frequencies within the corpus. The list of unigrams are sorted (in descending order) on the frequency of the unigram within the corpus.

Input Data
The input data for PA1 is a dataset compiled from the set of Wikipedia articles. Each data file is formatted as follows:

… 
Title_of_Article-1<====>DocumentID-1<====>Text_of_Article-1 
NEWLINE 
NEWLINE 
Title_of_Article-2<====>DocumentID-2<====>Text_of_Article-2 
…
There are 3 components describing an article: (1) Title of the article, (2) Document ID, and (3) Text of the article. The title (text information) represents the title of a Wikipedia article. The document ID is specified by Wikipedia and every article has a unique document id. Finally, the text of the article encapsulates what was included in that Wikipedia article. Each data file may contain multiple Wikipedia articles and these are separated by two consecutive NEWLINE characters.
