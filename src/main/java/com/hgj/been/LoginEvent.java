package com.hgj.been;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginEvent {
    private Long userId;
    private String ip;
    private String enentType;
    private Long eventTime;
}
