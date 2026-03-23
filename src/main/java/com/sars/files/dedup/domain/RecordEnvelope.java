package com.sars.files.dedup.domain;

//import tools.jackson.databind.JsonNode;

import tools.jackson.databind.JsonNode;

public record RecordEnvelope(FileDescriptor fileDescriptor,
                             long lineNumber,
                             String rawLine,
                             JsonNode jsonNode) {
}
