package com.hgj.been;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ItemCount {
    private Long item;
    private String time;
    private Integer count;
}
