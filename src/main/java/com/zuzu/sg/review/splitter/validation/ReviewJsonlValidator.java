package com.zuzu.sg.review.splitter.validation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class ReviewJsonlValidator {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(ReviewJsonlValidator.class);

    List<String> lineValidationErrors = new ArrayList<>();

    public List<String> validateReviewJsonl(String extractedLine)    {
        try {
            JsonNode jsonNode = objectMapper.readTree(extractedLine);
        } catch (JsonProcessingException e) {
            lineValidationErrors.add(String.format("Malformed JSON. Error: %s", e.getMessage()));
            log.error("Malformed JSON {}: {}", extractedLine, e.getMessage());
        } catch (Exception e) {
            lineValidationErrors.add(String.format("Error while processing the review jsonl: %s", e.getMessage()));
            log.error("Unexpected error on line {}: {}", extractedLine, e.getMessage(), e); // for other unknown errors
        }
        return lineValidationErrors;
    }


}
