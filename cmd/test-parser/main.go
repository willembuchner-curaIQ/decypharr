package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
)

func main() {
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	log := zerolog.New(output).With().Timestamp().Logger()
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: test-parser <nzb-file>")
		os.Exit(2)
	}

	nzbFile := os.Args[1]
	content, err := os.ReadFile(nzbFile)
	if err != nil {
		log.Fatal().Err(err).Str("file", nzbFile).Msg("Failed to read NZB file")
	}

	config.SetConfigPath("data/")
	client, err := nntp.NewClient(config.Get())
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create NNTP client")
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Warn().Err(err).Msg("Failed to close NNTP client")
		}
	}()

	p := parser.NewParser(client, 10, log)
	nzb, groups, err := p.Parse(context.Background(), nzbFile, content)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to parse NZB")
	}
	nzb, err = p.Process(context.Background(), nzb, groups)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to process NZB")
	}

	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("FILE SUMMARY")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("NZB ID:        %s\n", nzb.ID)
	fmt.Printf("Name:          %s\n", nzb.Name)
	fmt.Printf("Total Size:    %.2f GB\n", float64(nzb.TotalSize)/(1024*1024*1024))
	fmt.Printf("Logical Files: %d\n", len(nzb.Files))

	for i, file := range nzb.Files {
		fmt.Printf("\n[%d] %s\n", i+1, file.Name)
		fmt.Printf("    Size:         %.2f MB (%d bytes)\n", float64(file.Size)/(1024*1024), file.Size)
		fmt.Printf("    Segments:     %d\n", len(file.Segments))
		fmt.Printf("    Password:     %s\n", passwordStatus(file.Password))
		if file.InternalPath != "" {
			fmt.Printf("    Internal:     %s\n", file.InternalPath)
		}
		if file.IsStored {
			fmt.Println("    Compression:  Stored (seekable)")
		} else {
			fmt.Println("    Compression:  Compressed")
		}

		zeroBytes := 0
		for _, segment := range file.Segments {
			if segment.Bytes <= 0 {
				zeroBytes++
			}
		}
		if zeroBytes > 0 {
			fmt.Printf("    Zero-byte segments: %d\n", zeroBytes)
		}
	}

	log.Info().Msg("Parser test completed successfully")
}

func passwordStatus(password string) string {
	if password == "" {
		return "None"
	}
	return "Protected (***)"
}
