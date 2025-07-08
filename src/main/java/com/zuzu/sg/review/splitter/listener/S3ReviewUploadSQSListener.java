package com.zuzu.sg.review.splitter.listener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zuzu.sg.review.splitter.utility.S3JSONLSplitter;
import io.awspring.cloud.sqs.annotation.SqsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * S3ReviewUploadSQSListener to listen to review-upload-sqs queue.
 * When the user uploads a jsonl file to the sg-reviews-uploads-bucket s3 bucket, an SQS even is triggered.
 */
@Service
public class S3ReviewUploadSQSListener {
    private static final Logger log = LoggerFactory.getLogger(S3ReviewUploadSQSListener.class);
    @Autowired
    S3JSONLSplitter s3JSONLSplitter;

    @Autowired
    ObjectMapper objectMapper;

    @SqsListener("review-upload-sqs")
    public void receiveMessage(String sqsJsonMessage) {
        log.info("SQS event received from review-upload-sqs. Event json: " + sqsJsonMessage);
        s3JSONLSplitter.splitFile(getUploadedReviewFileName(sqsJsonMessage));
    }

    /**
     * Extracts the actual file name from the sg-reviews-uploads-bucket
     * @param sqsJsonMessage
     * @return the file name for which event got generated
     */
    private String getUploadedReviewFileName(String sqsJsonMessage) {
        try {
            JsonNode rootNode = objectMapper.readTree(sqsJsonMessage);
            JsonNode recordsNode = rootNode.path("Records");
            if (recordsNode.isArray()) {
                for (JsonNode record : recordsNode) {
                    String objectKey = record.path("s3").path("object").path("key").asText();

                    if (!objectKey.isEmpty()) {
                        return objectKey;
                    } else {
                        log.info("Could not extract object key from record: " + record);
                    }
                }
            } else {
                log.info("SQS message did not contain a 'Records' array or was empty.");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to process S3 event message", e);
        }
        return "";
    }
}

