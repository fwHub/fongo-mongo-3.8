package com.github.fakemongo.integration;

import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Map;

/**
 * @author Tom Dearman
 */
@Document
@CompoundIndexes({
        @CompoundIndex(name = "serial_date_idx", def = "{'barcode.serial': 1, 'barcode.time': 1}")
})
public class SpringModelMap
{
    private Map<String, Object> barcode;

    SpringModelMap(Map<String, Object> barcode) {
        this.barcode = barcode;
    }

    public Map<String, Object> getBarcode() {
        return barcode;
    }

    public void setBarcode( Map<String, Object> barcode) {
        this.barcode = barcode;
    }
}
