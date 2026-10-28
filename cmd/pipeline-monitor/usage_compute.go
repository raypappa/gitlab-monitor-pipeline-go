package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const projectComputePageSize = 100

var hostedRunnerCostFactors = map[string]float64{
	"saas-linux-small-amd64":               1,
	"saas-linux-medium-amd64":              2,
	"saas-linux-large-amd64":               3,
	"saas-linux-xlarge-amd64":              6,
	"saas-linux-2xlarge-amd64":             12,
	"saas-linux-medium-amd64-gpu-standard": 7,
	"saas-linux-small-arm64":               1,
	"saas-linux-medium-arm64":              2,
	"saas-linux-large-arm64":               3,
	"saas-macos-medium-m1":                 6,
	"saas-macos-large-m2pro":               12,
	"saas-windows-medium-amd64":            1,
}

func hostedRunnerCostFactor(job usageJob, host string) (float64, bool) {
	if job.Runner == nil || job.Runner.RunnerType != "instance_type" {
		return 0, false
	}
	var factor float64
	found := false
	for _, tag := range job.Tags {
		knownFactor, knownTag := hostedRunnerCostFactors[strings.ToLower(tag)]
		if !knownTag {
			continue
		}
		if found && factor != knownFactor {
			return 0, false
		}
		factor = knownFactor
		found = true
	}
	if !found && len(job.Tags) == 0 && isGitLabComHost(host) {
		// GitLab.com sends untagged jobs to the default small Linux x86-64 runner.
		return 1, true
	}
	return factor, found
}

func isGitLabComHost(host string) bool {
	parsed, err := url.Parse(host)
	return err == nil && strings.EqualFold(parsed.Hostname(), "gitlab.com")
}

func estimateJobComputeMinutes(durationSeconds, costFactor float64) float64 {
	return durationSeconds / 60 * costFactor
}

func projectComputeMonths(since, until time.Time, untilWasDefault bool, now time.Time) ([]time.Time, bool) {
	since = since.UTC()
	until = until.UTC()
	now = now.UTC()
	if since.Day() != 1 || since.Hour() != 0 || since.Minute() != 0 || since.Second() != 0 || since.Nanosecond() != 0 {
		return nil, false
	}
	monthEnd := time.Date(until.Year(), until.Month()+1, 1, 0, 0, 0, 0, time.UTC).Add(-time.Nanosecond)
	completeMonthRange := until.Equal(monthEnd)
	currentMonthToDate := untilWasDefault && until.Year() == now.Year() && until.Month() == now.Month()
	currentMonthEndWindow := until.After(now) && until.Year() == now.Year() && until.Month() == now.Month()
	if !completeMonthRange && !currentMonthToDate && !currentMonthEndWindow {
		return nil, false
	}
	firstMonth := time.Date(since.Year(), since.Month(), 1, 0, 0, 0, 0, time.UTC)
	lastMonth := time.Date(until.Year(), until.Month(), 1, 0, 0, 0, 0, time.UTC)
	if lastMonth.Before(firstMonth) {
		return nil, false
	}
	months := make([]time.Time, 0, (lastMonth.Year()-firstMonth.Year())*12+int(lastMonth.Month()-firstMonth.Month())+1)
	for month := firstMonth; !month.After(lastMonth); month = month.AddDate(0, 1, 0) {
		months = append(months, month)
	}
	return months, true
}

func formatComputeUsagePeriod(months []time.Time) string {
	if len(months) == 0 {
		return ""
	}
	if len(months) == 1 {
		return months[0].Format("2006-01")
	}
	return months[0].Format("2006-01") + " through " + months[len(months)-1].Format("2006-01")
}

func projectComputeNamespaceIDs(groupID int64, projects []groupProject) []int64 {
	seen := map[int64]bool{groupID: true}
	namespaceIDs := []int64{groupID}
	for _, project := range projects {
		namespaceID := project.Namespace.ID
		if namespaceID == 0 || seen[namespaceID] {
			continue
		}
		seen[namespaceID] = true
		namespaceIDs = append(namespaceIDs, namespaceID)
	}
	return namespaceIDs
}

func (c *client) groupProjectComputeUsage(ctx context.Context, namespaceIDs []int64, months []time.Time) (map[int64]int64, error) {
	minutesByProject := make(map[int64]int64)
	const query = `query($namespaceId: NamespaceID!, $date: Date!, $after: String) {
  ciMinutesProjectMonthlyUsage(namespaceId: $namespaceId, date: $date, first: 100, after: $after) {
    nodes { minutes project { id name } }
    pageInfo { endCursor hasNextPage }
  }
}`
	for _, month := range months {
		for _, namespaceID := range namespaceIDs {
			after := ""
			for {
				variables := map[string]string{
					"namespaceId": fmt.Sprintf("gid://gitlab/Group/%d", namespaceID),
					"date":        month.Format("2006-01-02"),
				}
				if after != "" {
					variables["after"] = after
				}
				request := map[string]any{"query": query, "variables": variables}
				var response ciMinutesProjectUsageResponse
				if err := c.postJSON(ctx, "/api/graphql", request, &response); err != nil {
					return nil, err
				}
				if len(response.Errors) > 0 {
					messages := make([]string, 0, len(response.Errors))
					for _, gqlError := range response.Errors {
						if gqlError.Message != "" {
							messages = append(messages, gqlError.Message)
						}
					}
					if len(messages) == 0 {
						return nil, errors.New("GitLab GraphQL did not return project compute usage")
					}
					return nil, fmt.Errorf("GitLab project compute usage: %s", strings.Join(messages, "; "))
				}
				usage := response.Data.Usage
				for _, projectUsage := range usage.Nodes {
					if projectUsage.Minutes == nil {
						continue
					}
					projectID, err := parseGlobalProjectID(projectUsage.Project.ID)
					if err != nil {
						return nil, fmt.Errorf("parse project compute usage ID: %w", err)
					}
					minutesByProject[projectID] += *projectUsage.Minutes
				}
				if !usage.PageInfo.HasNextPage {
					break
				}
				if usage.PageInfo.EndCursor == "" || usage.PageInfo.EndCursor == after {
					return nil, errors.New("GitLab project compute usage pagination did not advance")
				}
				after = usage.PageInfo.EndCursor
			}
		}
	}
	return minutesByProject, nil
}

func parseGlobalProjectID(globalID string) (int64, error) {
	const prefix = "gid://gitlab/Project/"
	if !strings.HasPrefix(globalID, prefix) {
		return 0, fmt.Errorf("unexpected global ID %q", globalID)
	}
	rawID := strings.TrimPrefix(globalID, prefix)
	if rawID == "" || strings.Contains(rawID, "/") {
		return 0, fmt.Errorf("unexpected global ID %q", globalID)
	}
	projectID, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid global ID %q: %w", globalID, err)
	}
	return projectID, nil
}
