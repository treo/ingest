package tech.dubs.ingest.functions.conceptnet;

import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ConceptNetWordVectors {
    private INDArray wordMatrix;

    public static ConceptNetWordVectors load(String path) throws IOException {
        BufferedReader reader = Files.newBufferedReader(Paths.get(path), Charset.forName("UTF-8"));
        String header = reader.readLine();
        String[] split = header.split(" ");
        Integer wordCount = Integer.valueOf(split[0]);
        Integer dimensions = Integer.valueOf(split[1]);

        ConceptNetWordVectors wv = new ConceptNetWordVectors();
        wv.wordMatrix = Nd4j.create(wordCount, dimensions);
        String line;
        int idx = 0;
        while((line = reader.readLine()) != null){
            String[] parts = line.split(" ", 2);
            String[] valueStrings = parts[1].split(" ");
            double[] values = new double[valueStrings.length];
            for (int i = 0; i < valueStrings.length; i++) {
                values[i] = Double.valueOf(valueStrings[i]);
            }
            wv.wordMatrix.putRow(idx, Nd4j.create(values));
            idx++;
        }

        return wv;
    }

    public static ConceptNetWordVectors loadBinary(String path) throws IOException {
        ConceptNetWordVectors wv = new ConceptNetWordVectors();
        wv.wordMatrix = Nd4j.readBinary(Paths.get(path).toFile());
        return wv;
    }


    public void saveBinary(String path) throws IOException {
        Nd4j.saveBinary(wordMatrix, Paths.get(path).toFile());
    }


    public INDArray getWord(int idx){
        return wordMatrix.getRow(idx);
    }

    public INDArray getWordMatrix() {
        return wordMatrix;
    }
}
