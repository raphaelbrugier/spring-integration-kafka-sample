package com.github.rbrugier.esb;

import java.io.Serializable;

public class MessageWrapper implements Serializable {

    private final String groupId;
    private final String commandId;
    private final String value;

    public MessageWrapper(String commandId, String groupId, String value) {
        this.commandId = commandId;
        this.groupId = groupId;
        this.value = value;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getCommandId() {
        return commandId;
    }

    public String getValue() {
        return value;
    }
}
