package tech.dubs.ingest.functions;

import tech.dubs.ingest.functions.generic.SubsetOfColumnsFunction;

import java.util.Map;

public class RenameColumns<T, U, X> extends SubsetOfColumnsFunction<T, U, X, X> {
    private final Map<T, U> renameMappings;

    public RenameColumns(Map<T,U> renameMappings) {
        super(renameMappings.keySet().toArray((T[])new Object[0]));
        this.renameMappings = renameMappings;
    }

    @Override
    protected void parseColumn(Map<T, X> map, T oldColumnName) {
        Map<U,X> casted = (Map<U,X>) map;
        X val = map.get(oldColumnName);
        map.remove(oldColumnName);
        casted.put(renameMappings.get(oldColumnName), val);
    }
}
