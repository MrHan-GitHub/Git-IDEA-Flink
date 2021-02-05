package com.hgj.been;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.checkerframework.checker.units.qual.A;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class wordToOne {
    private String word;
    private Integer count;
}
