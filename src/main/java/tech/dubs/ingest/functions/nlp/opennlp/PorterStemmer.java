package tech.dubs.ingest.functions.nlp.opennlp;


import opennlp.tools.stemmer.Stemmer;

public class PorterStemmer extends AbstractStemmer {

    private final opennlp.tools.stemmer.PorterStemmer stemmer;

    public PorterStemmer() {
        this.stemmer = new opennlp.tools.stemmer.PorterStemmer();
    }

    @Override
    protected Stemmer getStemmer() {
        return stemmer;
    }
}
