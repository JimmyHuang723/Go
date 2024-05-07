package main

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

type DownloadDescriptor struct {
	fileURL          string
	fileDownloadPath string
	start            int64
	end              int64
	chunkIndex       int
}

func calculateETag(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

func calculateChunkSize(fileSize int64, defaultChunkSize int64, maxNumOfWorkers int) int64 {
	resultChunkSize := defaultChunkSize
	numOfWorkers := int(math.Ceil(float64(fileSize) / float64(defaultChunkSize)))
	if numOfWorkers > maxNumOfWorkers {
		// recalculate chunkSize as it needs to limit the max number of workers
		resultChunkSize = fileSize / int64(maxNumOfWorkers)
	}
	return resultChunkSize
}

func worker(wg *sync.WaitGroup, download *DownloadDescriptor, resultChan chan error) {
	defer wg.Done()

	start := download.start
	end := download.end
	i := download.chunkIndex

	req, err := http.NewRequest("GET", download.fileURL, nil)
	if err != nil {
		fmt.Println("Error creating request:", err)
		resultChan <- err
		return
	}

	// Request only a portion of the resource using the Range header,
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error downloading chunk %d: %s\n", i, err)
		resultChan <- err
		return
	}
	defer resp.Body.Close()
	// Make sure response status code is 206 (Partial Content) for each chunk,
	// indicating that the server accepts byte-range requests and is responding with the requested chunk.
	if resp.StatusCode != http.StatusPartialContent {
		resultChan <- errors.New(fmt.Sprintf("Unexpected status code %d for chunk %d\n", resp.StatusCode, i))
		return
	}

	// Open file for each chunk to write to
	outFileChunk, err := os.OpenFile(download.fileDownloadPath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("Error opening file chunk ", err)
		resultChan <- err
		return
	}
	defer outFileChunk.Close()

	// Seek the file to the correct position to write
	_, err = outFileChunk.Seek(start, 0)
	if err != nil {
		fmt.Printf("Error seeking to position %d for chunk %d: %s\n", start, i, err)
		resultChan <- err
		return
	}

	// Write the chunk
	n, err := io.Copy(outFileChunk, resp.Body)
	if err != nil {
		fmt.Printf("Error writing chunk %d to file: %s\n", i, err)
		resultChan <- err
		return
	}
	if n != end-start+1 {
		resultChan <- errors.New(fmt.Sprintf("Error: wrote %d bytes, expected %d\n", n, end-start+1))
		return
	} else {
		fmt.Printf("Wrote %d bytes for chunk %d\n", n, i)
	}

	resultChan <- nil
	return // success
}

func main() {

	defaultChunkSize := int64(1024 * 1024) // This can be configured to optimize for network condition.
	maxNumOfWorkers := 30                  // Max concurrently running workers that download chunks.
	fileURL := ""
	fileDownloadPath := ""

	// Get and validate user input
	if len(os.Args) > 2 {
		fileURL = os.Args[1]
		u, err := url.ParseRequestURI(fileURL)
		if err != nil || u.Scheme == "" || u.Host == "" {
			fmt.Printf("Invalid arg 'fileURL'. Err : %s\n", err)
			return
		}
		fileDownloadDir := os.Args[2]
		if _, err := os.Stat(fileDownloadDir); err != nil {
			if os.IsNotExist(err) {
				fmt.Printf("Input arg 'fileDownloadDir' doesn't exist. Err : %s\n", err)
				return
			}
		}
		fileDownloadPath = filepath.Join(fileDownloadDir, path.Base(fileURL))
		fmt.Printf("Downloading URL : %s \nto %s\n", fileURL, fileDownloadPath)
	} else {
		fmt.Println("Arguments insufficient!")
		fmt.Println("[Usage]")
		fmt.Println("    go run download.go [fileURL] [fileDownloadDir]")
		fmt.Println("    e.g. go run download.go https://.../xxx.zip /local/filepath")
		return
	}

	// Determine file size
	resp, err := http.Head(fileURL)
	if err != nil {
		fmt.Println("Error determining file size:", err)
		return
	}
	defer resp.Body.Close()
	fileSize := resp.ContentLength
	if fileSize <= 0 {
		fmt.Println("Could not determine file size.")
		return
	} else {
		fmt.Printf("File size %d\n", fileSize)
	}

	// Create a tmp file to write the downloaded chunks to
	incompleteFileDownloadPath := fileDownloadPath + ".incomplete"
	outFile, err := os.Create(incompleteFileDownloadPath)
	if err != nil {
		fmt.Println("Error creating output file:", err)
		return
	}
	defer outFile.Close()

	// Pre-allocate file size on storage
	if err := outFile.Truncate(fileSize); err != nil {
		fmt.Println("Error pre-allocating file size:", err)
		return
	}

	// Calculate chunkSize based on fileSize to avoid large files using too many workers
	chunkSize := calculateChunkSize(fileSize, defaultChunkSize, maxNumOfWorkers)

	// Calculate num of workers that will run in parallel
	numWorkers := int(math.Ceil(float64(fileSize) / float64(chunkSize)))
	fmt.Printf("chunkSize %d\n", chunkSize)
	fmt.Printf("Num of workers %d\n", numWorkers)

	// Download chunks in parallel with multiple works
	resultChan := make(chan error, numWorkers) // Use channel to communicate with workers
	var wg sync.WaitGroup                      // Use a WaitGroup to wait for all workers to finish
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		start := int64(i) * int64(chunkSize)
		end := start + int64(chunkSize) - 1
		if i == numWorkers-1 {
			end = fileSize - 1
		}

		go worker(&wg, &DownloadDescriptor{
			fileURL:          fileURL, //  URL of each worker may be different if the file is hosted on different servers
			fileDownloadPath: incompleteFileDownloadPath,
			start:            start,
			end:              end,
			chunkIndex:       i,
		},
			resultChan)
	}

	// Wait for all workers to finish
	wg.Wait()

	// Check if all workers are successful.
	allWorkerSuccess := (len(resultChan) == numWorkers)
	for i := 0; i < numWorkers; i++ {
		result := <-resultChan
		if result != nil {
			allWorkerSuccess = false // failed if any one of the workers has error
			fmt.Printf("Error %s on some chunk.", result)
		}
	}
	if !allWorkerSuccess {
		fmt.Println("Download failed!")
		return
	}

	// Rename the file once it is complete
	errReName := os.Rename(incompleteFileDownloadPath, fileDownloadPath)
	if errReName != nil {
		fmt.Println("Error renaming file:", err)
		return
	}

	// Verify ETAG
	serverETAG := resp.Header.Get("ETag")
	if serverETAG != "" {
		serverETAG = strings.Trim(serverETAG, `"`)
		localETag, err := calculateETag(fileDownloadPath)
		if err == nil {
			if serverETAG == localETag {
				fmt.Println("ETags match!")
			} else {
				// Download may be successful even though ETag doesn't match.
				fmt.Println("ETags do not match (This doesn't mean download is unsuccessful).")
				fmt.Printf("Local ETag: %s\nServer ETag: %s\n", localETag, serverETAG)
			}
		} else {
			fmt.Printf("Failed to generate ETag for file %s\n", fileDownloadPath)
		}
	}

	fmt.Println("Download complete.")
}
