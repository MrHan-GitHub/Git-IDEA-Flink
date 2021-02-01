package com.hgj.been;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UrlCount {
    private String url;
    private Long windowEnd;
    private Integer count;
}
