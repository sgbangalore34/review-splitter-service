package com.zuzu.sg.review.splitter.utility;

import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.zuzu.sg.review.splitter.exception.S3FileProcessingException;
import com.zuzu.sg.review.splitter.validation.ReviewJsonlValidator;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;

/**
 * The splitter utility to split one large jsonl file to multiple based on split-file-lines-per-chunk setting
 */
@Service
public class S3JSONLSplitter {

    private static final Logger log = LoggerFactory.getLogger(S3JSONLSplitter.class);

    @Autowired
    S3Client s3Client;
    private final TransferManager transferManager;

    @Autowired
    ReviewJsonlValidator reviewJsonlValidator;

    @Value("${aws.s3.source-bucket-name}")
    private String sourceBucketName;

    @Value("${aws.s3.destination-bucket-name}")
    private String destinationBucketName;

    @Value("${aws.s3.archive-bucket-name}")
    private String archiveBucketName;

    @Value("${split-file-lines-per-chunk}")
    private int linesPerChunk;

    public S3JSONLSplitter() {
        this.transferManager = TransferManagerBuilder.standard()
                .withExecutorFactory(() -> Executors.newFixedThreadPool(5))
                .build(); //executors initialized for parallel processing
    }

    /**
     * Handles the splitting and moving of file to the destination bucket with archiving the processed file
     * @param inputFile
     */
    public void splitFile(String inputFile) {
        CompletableFuture.supplyAsync(() -> {
            List<String> uploadedSplitFiles = new ArrayList<>();
            Path tempFile = null;

            try {
                tempFile = downloadFileFromS3(inputFile);

                try (BufferedReader reader = Files.newBufferedReader(tempFile)) {
                    String line;
                    List<String> currentChunkLines = new ArrayList<>();
                    int chunkNumber = 1;

                    List<CompletableFuture<String>> chunkUploadFutures = new ArrayList<>();

                    while ((line = reader.readLine()) != null) {
                        List<String> validationErrors = reviewJsonlValidator.validateReviewJsonl(line);
                        if(validationErrors.isEmpty()) { // this means the json is not well formed
                            currentChunkLines.add(line);

                            if (currentChunkLines.size() >= linesPerChunk) {
                                String splitFileName = generateSplitFileName(inputFile, chunkNumber);
                                chunkUploadFutures.add(uploadChunkAsync(currentChunkLines, splitFileName));
                                uploadedSplitFiles.add(splitFileName);
                                currentChunkLines.clear();
                                chunkNumber++;
                            }
                        }
                    }

                    if (!currentChunkLines.isEmpty()) {
                        String chunkFileName = generateSplitFileName(inputFile, chunkNumber);
                        chunkUploadFutures.add(uploadChunkAsync(currentChunkLines, chunkFileName));
                        uploadedSplitFiles.add(chunkFileName);
                    }

                    CompletableFuture.allOf(chunkUploadFutures.toArray(new CompletableFuture[0])).join();

                    log.info("Finished splitting and uploading files for '{}'. Total chunks uploaded: {}", inputFile, uploadedSplitFiles.size());

                    moveSourceFileToArchiveBucket(inputFile);
                    return uploadedSplitFiles;
                }

            } catch (IOException e) {
                log.error("Error splitting or uploading JSONL file: {}", e.getMessage(), e);
                throw new CompletionException("Failed to process JSONL file: " + e.getMessage(), e);
            } finally {
                if (tempFile != null) {
                    try {
                        Files.deleteIfExists(tempFile);
                        log.info("Cleaning up the temporary jl file created: {}", tempFile);
                    } catch (IOException e) {
                        log.warn("Failed to delete temporary file {}: {}", tempFile, e.getMessage());
                    }
                }

                if (transferManager != null) {
                    transferManager.shutdownNow(true);
                }
            }
        });
    }

    /**
     * Once the splitting is complete, the file is moved to archive bucket for auditing
     * Copy and delete is followed
     * @param inputFile
     */
    private void moveSourceFileToArchiveBucket(String inputFile) {
        s3Client.copyObject(request -> request.sourceBucket(sourceBucketName).sourceKey(inputFile).destinationBucket(archiveBucketName).destinationKey(inputFile + "_" + LocalDateTime.now()));
        DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(sourceBucketName, inputFile);
        s3Client.deleteObject(request -> request.bucket(sourceBucketName).key(inputFile));
    }

    private Path downloadFileFromS3(String inputFile) throws IOException {
        Path tempFile = Paths.get("uploaded-reviews.jl");
        log.info("Getting file '{}' from S3 bucket '{}'", inputFile, sourceBucketName);
        try {
        s3Client.getObject(request -> request.bucket(sourceBucketName).key(inputFile), ResponseTransformer.toFile(tempFile));
            log.info("Downloaded the input file : '{}'", inputFile);
            return tempFile;
        } catch (S3Exception e) {
            log.error("S3 service exception during download of {}: {}", inputFile, e.getMessage());
            throw e;
        } catch (SdkClientException e) {
            log.error("AWS SDK client exception during download of {}: {}", inputFile, e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Unexpected error during S3 download of {}: {}", inputFile, e.getMessage());
            throw new S3FileProcessingException("Unexpected error during S3 download of " + inputFile, e);
        }


//        Path tempFile = Paths.get("temp-file.jl"); //temp file name for processing
//        log.info("Getting file '{}' from S3 bucket '{}'", inputFile, sourceBucketName);
//        try {
//        s3Client.getObject(request -> request.bucket(sourceBucketName).key(inputFile), ResponseTransformer.toFile(tempFile));
//            log.info("Downloaded the input file : '{}'", inputFile);
//            return tempFile;
//        } catch (S3Exception e) {
//            log.error("S3 service exception during download of {}: {}", inputFile, e.getMessage());
//            throw e;
//        } catch (SdkClientException e) {
//            log.error("AWS SDK client exception during download of {}: {}", inputFile, e.getMessage());
//            throw e;
//        } catch (Exception e) {
//            log.error("Unexpected error during S3 download of {}: {}", inputFile, e.getMessage());
//            throw new S3FileProcessingException("Unexpected error during S3 download of " + inputFile, e);
//        }
    }

    /**
     * Executor threads pick the files and upload them to the destination reducer bucket
     * @param chunkLines
     * @param chunkFileName
     * @return
     */
    private CompletableFuture<String> uploadChunkAsync(List<String> chunkLines, String chunkFileName) {
        return CompletableFuture.supplyAsync(() -> {
            log.info("Uploading chunk: {} with {} lines", chunkFileName, chunkLines.size());
            Path chunkTempFile = null;
            try {
                chunkTempFile = Files.createTempFile("jsonl-chunk-", ".jl");
                Files.write(chunkTempFile, chunkLines, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentType("application/jsonl");
                metadata.setContentLength(Files.size(chunkTempFile));

                PutObjectRequest putObjectRequest = new PutObjectRequest(
                        destinationBucketName, chunkFileName, chunkTempFile.toFile());
                putObjectRequest.setMetadata(metadata);

                Upload upload = transferManager.upload(putObjectRequest);
                upload.waitForCompletion();
                log.info("Successfully uploaded chunk to destination bucket: {}", chunkFileName);
                return chunkFileName; // Return the key of the uploaded chunk
            } catch (IOException | InterruptedException e) {
                log.error("Error creating or uploading chunk {}: {}", chunkFileName, e.getMessage(), e);
                Thread.currentThread().interrupt(); // Restore interrupted status
                throw new CompletionException("Failed to upload chunk: " + chunkFileName, e);
            } finally {
                if (chunkTempFile != null) {
                    try {
                        Files.deleteIfExists(chunkTempFile);
                    } catch (IOException e) {
                        log.warn("Failed to delete chunk temporary file {}: {}", chunkTempFile, e.getMessage());
                    }
                }
            }
        });
    }

    /**
     * @param fileName Original file name in the uploads bucket
     * @param chunkNumber
     * @return String
     */
    private String generateSplitFileName(String fileName, int chunkNumber) {
        String fileNameWithoutExtension = fileName;
        String fileExtension = "";
        int dotIndex = fileName.lastIndexOf('.');
        if (dotIndex > 0) {
            fileNameWithoutExtension = fileName.substring(0, dotIndex);
            fileExtension = fileName.substring(dotIndex);
        }
        return String.format("%s-split-%d%s", fileNameWithoutExtension, chunkNumber, fileExtension);
    }
}