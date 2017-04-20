package tech.dubs.ingest.functions.nlp.opennlp;


import opennlp.tools.stemmer.Stemmer;
import opennlp.tools.stemmer.snowball.SnowballStemmer.ALGORITHM;


public class SnowballStemmer extends AbstractStemmer {

    private final opennlp.tools.stemmer.snowball.SnowballStemmer stemmer;

    public SnowballStemmer(ALGORITHM algorithm) {
        this.stemmer = new opennlp.tools.stemmer.snowball.SnowballStemmer(algorithm);
    }

    @Override
    protected Stemmer getStemmer() {
        return stemmer;
    }
}
