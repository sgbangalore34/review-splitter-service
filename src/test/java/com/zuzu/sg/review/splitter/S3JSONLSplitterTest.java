package com.zuzu.sg.review.splitter;

import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.zuzu.sg.review.splitter.utility.S3JSONLSplitter;
import com.zuzu.sg.review.splitter.validation.ReviewJsonlValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.util.ReflectionTestUtils;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class S3JSONLSplitterTest {

    @Mock
    private S3Client s3Client;

    @Mock
    private ReviewJsonlValidator reviewJsonlValidator;

    @Mock
    private TransferManager transferManager;

    @Mock
    private Upload upload;

    @InjectMocks
    private S3JSONLSplitter s3JsonlSplitter;

    @TempDir
    Path tempDir;

    private final String SOURCE_BUCKET = "test-source-bucket";
    private final String DESTINATION_BUCKET = "test-destination-bucket";
    private final String ARCHIVE_BUCKET = "test-archive-bucket";
    private final int LINES_PER_CHUNK = 2;

    private Path mockDownloadedFilePath;

    @BeforeEach
    void setUp() throws IOException, InterruptedException {
        // Initialize S3JSONLSplitter and inject mocks/values
        s3JsonlSplitter = new S3JSONLSplitter();
        ReflectionTestUtils.setField(s3JsonlSplitter, "s3Client", s3Client);
        ReflectionTestUtils.setField(s3JsonlSplitter, "reviewJsonlValidator", reviewJsonlValidator);
        ReflectionTestUtils.setField(s3JsonlSplitter, "transferManager", transferManager);
        ReflectionTestUtils.setField(s3JsonlSplitter, "sourceBucketName", SOURCE_BUCKET);
        ReflectionTestUtils.setField(s3JsonlSplitter, "destinationBucketName", DESTINATION_BUCKET);
        ReflectionTestUtils.setField(s3JsonlSplitter, "archiveBucketName", ARCHIVE_BUCKET);
        ReflectionTestUtils.setField(s3JsonlSplitter, "linesPerChunk", LINES_PER_CHUNK);

        // Common stubbings for successful scenarios
        when(transferManager.upload(any(PutObjectRequest.class))).thenReturn(upload);
        // Default for void method waitForCompletion() is doNothing(), no explicit stubbing needed
        // doNothing().when(upload).waitForCompletion(); // Can be removed

        // Create a mock file path for download to simplify individual test setups
        mockDownloadedFilePath = tempDir.resolve("temp-file.jl");
        Files.createFile(mockDownloadedFilePath); // Ensure it exists for `toFile` transformer

        // Common stubbing for S3Client.getObject to simulate download
        doAnswer(invocation -> {
            Consumer<ResponseTransformer<software.amazon.awssdk.services.s3.model.GetObjectResponse, ?>> transformer = invocation.getArgument(1);
            transformer.accept(ResponseTransformer.toFile(mockDownloadedFilePath));
            return null;
        }).when(s3Client).getObject(any(Consumer.class), any(ResponseTransformer.class));

        // Common stubbing for validator (assumes valid JSONL by default)
        when(reviewJsonlValidator.validateReviewJsonl(anyString())).thenReturn(Collections.emptyList());
    }

    @Test
    @DisplayName("Splits and uploads file with multiple chunks")
    void splitFile_multipleChunksSuccess() throws IOException, InterruptedException {
        String inputFile = "input.jsonl";
        Files.write(mockDownloadedFilePath, "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n{\"id\":4}\n{\"id\":5}".getBytes());

        s3JsonlSplitter.splitFile(inputFile);

        verify(s3Client).getObject(any(Consumer.class), any(ResponseTransformer.class));
        verify(transferManager, times(3)).upload(any(PutObjectRequest.class));
        verify(upload, times(3)).waitForCompletion();

        // Verify content and names of captured PutObjectRequests
        ArgumentCaptor<PutObjectRequest> putRequestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(transferManager, times(3)).upload(putRequestCaptor.capture());
        java.util.List<PutObjectRequest> capturedRequests = putRequestCaptor.getAllValues();
        assertEquals(Arrays.asList("{\"id\":1}", "{\"id\":2}"), Files.readAllLines(capturedRequests.get(0).getFile().toPath()));
        assertEquals(Arrays.asList("{\"id\":3}", "{\"id\":4}"), Files.readAllLines(capturedRequests.get(1).getFile().toPath()));
        assertEquals(Collections.singletonList("{\"id\":5}"), Files.readAllLines(capturedRequests.get(2).getFile().toPath()));
        assertTrue(capturedRequests.get(0).getKey().startsWith("input-split-1.jsonl"));
        assertTrue(capturedRequests.get(1).getKey().startsWith("input-split-2.jsonl"));
        assertTrue(capturedRequests.get(2).getKey().startsWith("input-split-3.jsonl"));

        verifyFileArchivedAndDeleted(inputFile);
        assertFalse(Files.exists(mockDownloadedFilePath));
    }

    @Test
    @DisplayName("Handles empty input file")
    void splitFile_emptyInput() throws IOException, InterruptedException {
        String inputFile = "empty.jsonl";
        Files.write(mockDownloadedFilePath, new byte[0]); // Ensure file is empty

        s3JsonlSplitter.splitFile(inputFile);

        verify(transferManager, never()).upload(any(PutObjectRequest.class));
        verify(upload, never()).waitForCompletion();
        verifyFileArchivedAndDeleted(inputFile);
        assertFalse(Files.exists(mockDownloadedFilePath));
    }

    @Test
    @DisplayName("Processes file with fewer lines than chunk size")
    void splitFile_singleChunk() throws IOException, InterruptedException {
        String inputFile = "small.jsonl";
        Files.write(mockDownloadedFilePath, "{\"id\":1}\n{\"id\":2}".getBytes());

        s3JsonlSplitter.splitFile(inputFile);

        verify(transferManager, times(1)).upload(any(PutObjectRequest.class));
        verify(upload, times(1)).waitForCompletion();
        verifyFileArchivedAndDeleted(inputFile);
        assertFalse(Files.exists(mockDownloadedFilePath));
    }

    @Test
    @DisplayName("Handles lines with validation errors (adds them to chunk)")
    void splitFile_validationFailsAddsToChunk() throws IOException, InterruptedException {
        String inputFile = "invalid.jsonl";
        Files.write(mockDownloadedFilePath, "{\"id\":1}\nINVALID_JSON\n{\"id\":3}".getBytes());

        when(reviewJsonlValidator.validateReviewJsonl("{\"id\":1}")).thenReturn(Collections.emptyList());
        when(reviewJsonlValidator.validateReviewJsonl("INVALID_JSON")).thenReturn(Collections.singletonList("Invalid JSON"));
        when(reviewJsonlValidator.validateReviewJsonl("{\"id\":3}")).thenReturn(Collections.emptyList());

        s3JsonlSplitter.splitFile(inputFile);

        verify(transferManager, times(2)).upload(any(PutObjectRequest.class));
        verify(upload, times(2)).waitForCompletion();

        ArgumentCaptor<PutObjectRequest> putRequestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(transferManager, times(2)).upload(putRequestCaptor.capture());
        java.util.List<PutObjectRequest> capturedRequests = putRequestCaptor.getAllValues();
        assertEquals(Arrays.asList("{\"id\":1}", "INVALID_JSON"), Files.readAllLines(capturedRequests.get(0).getFile().toPath()));
        assertEquals(Collections.singletonList("{\"id\":3}"), Files.readAllLines(capturedRequests.get(1).getFile().toPath()));
    }

    @Test
    @DisplayName("Throws exception on S3 download error (S3Exception)")
    void splitFile_downloadS3Exception() {
        String inputFile = "error.jsonl";
        when(s3Client.getObject(any(Consumer.class), any(ResponseTransformer.class)))
                .thenThrow(S3Exception.builder().message("Test S3 Error").build());

        assertThrows(CompletionException.class, () -> s3JsonlSplitter.splitFile(inputFile));
        verify(s3Client, never()).copyObject(any(Consumer.class)); // No archive on error
    }

    @Test
    @DisplayName("Throws exception on S3 download error (SdkClientException)")
    void splitFile_downloadSdkClientException() {
        String inputFile = "error.jsonl";
        when(s3Client.getObject(any(Consumer.class), any(ResponseTransformer.class)))
                .thenThrow(SdkClientException.create("Test SDK Client Error"));

        assertThrows(CompletionException.class, () -> s3JsonlSplitter.splitFile(inputFile));
        verify(s3Client, never()).copyObject(any(Consumer.class));
    }

    @Test
    @DisplayName("Throws exception on chunk upload error (IOException)")
    void splitFile_chunkUploadIOException() throws IOException, InterruptedException {
        String inputFile = "input.jsonl";
        Files.write(mockDownloadedFilePath, "{\"id\":1}\n{\"id\":2}".getBytes());

        when(transferManager.upload(any(PutObjectRequest.class)))
                .thenThrow(new RuntimeException(new IOException("Simulated chunk write error")));

        assertThrows(CompletionException.class, () -> s3JsonlSplitter.splitFile(inputFile));
        verify(upload, never()).waitForCompletion(); // waitForCompletion was never successfully called
        verify(s3Client, never()).copyObject(any(Consumer.class));
        assertFalse(Files.exists(mockDownloadedFilePath)); // Temp file still cleaned up
    }

    @Test
    @DisplayName("Throws exception on chunk upload interrupted")
    void splitFile_chunkUploadInterruptedException() throws IOException, InterruptedException {
        String inputFile = "input.jsonl";
        Files.write(mockDownloadedFilePath, "{\"id\":1}\n{\"id\":2}".getBytes());

        doThrow(new InterruptedException("Simulated interrupted during upload")).when(upload).waitForCompletion();

        assertThrows(CompletionException.class, () -> s3JsonlSplitter.splitFile(inputFile));
        verify(upload, atLeastOnce()).waitForCompletion(); // Called but threw
        verify(s3Client, never()).copyObject(any(Consumer.class));
        assertFalse(Files.exists(mockDownloadedFilePath));
    }

    @Test
    @DisplayName("Generates split file name with extension")
    void generateSplitFileName_withExtension() {
        assertEquals("myreviews-split-5.jsonl",
                ReflectionTestUtils.invokeMethod(s3JsonlSplitter, "generateSplitFileName", "myreviews.jsonl", 5));
    }

    @Test
    @DisplayName("Generates split file name without extension")
    void generateSplitFileName_noExtension() {
        assertEquals("myreviews-split-1",
                ReflectionTestUtils.invokeMethod(s3JsonlSplitter, "generateSplitFileName", "myreviews", 1));
    }

    @Test
    @DisplayName("Moves source file to archive bucket and deletes it")
    void moveSourceFileToArchiveBucket_verifiesCalls() {
        String inputFile = "processed-file.jsonl";
        // No explicit stubbing needed for void methods.
        ReflectionTestUtils.invokeMethod(s3JsonlSplitter, "moveSourceFileToArchiveBucket", inputFile);
        verifyFileArchivedAndDeleted(inputFile);
    }

    /**
     * Helper method to verify archive and delete operations
     */
    private void verifyFileArchivedAndDeleted(String inputFile) {
        ArgumentCaptor<Consumer<CopyObjectRequest.Builder>> copyRequestCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(s3Client).copyObject(copyRequestCaptor.capture());
        CopyObjectRequest.Builder copyBuilder = CopyObjectRequest.builder();
        copyRequestCaptor.getValue().accept(copyBuilder); // Correct usage: pass builder to consumer
        CopyObjectRequest copyRequest = copyBuilder.build();

        assertEquals(SOURCE_BUCKET, copyRequest.sourceBucket());
        assertEquals(inputFile, copyRequest.sourceKey());
        assertEquals(ARCHIVE_BUCKET, copyRequest.destinationBucket());
        assertTrue(copyRequest.destinationKey().startsWith(inputFile + "_"));

        ArgumentCaptor<Consumer<DeleteObjectRequest.Builder>> deleteRequestCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(s3Client).deleteObject(deleteRequestCaptor.capture());
        DeleteObjectRequest.Builder deleteBuilder = DeleteObjectRequest.builder();
        deleteRequestCaptor.getValue().accept(deleteBuilder); // Correct usage: pass builder to consumer
        DeleteObjectRequest deleteRequest = deleteBuilder.build();

        assertEquals(SOURCE_BUCKET, deleteRequest.bucket());
        assertEquals(inputFile, deleteRequest.key());
    }
}