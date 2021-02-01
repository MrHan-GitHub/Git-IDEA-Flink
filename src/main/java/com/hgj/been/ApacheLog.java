package com.hgj.been;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ApacheLog {
    private String ip;
    private String uid;
    private Long ts;
    private String method;
    private String url;
}
