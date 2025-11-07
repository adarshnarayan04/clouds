package org.cloudbus.cloudsim.cclab;

import java.util.*;
import java.text.DecimalFormat;

public class perf_rr {

    static class ExecutionStep {
        int cloudletId;
        int vmId;
        double startTime;
        double endTime;
        long executedLength;

        ExecutionStep(int cloudletId, int vmId, double startTime, double endTime, long executedLength) {
            this.cloudletId = cloudletId;
            this.vmId = vmId;
            this.startTime = startTime;
            this.endTime = endTime;
            this.executedLength = executedLength;
        }
    }

    public static void main(String[] args) {
        System.out.println("Q2. Round Robin Scheduling");
        System.out.println("Inputs: Cloudlets [2000, 3000, 4000, 5000, 6000, 7000], VMs: 2 (1000 MIPS each)");

        long[] cloudletLengths = {2000, 3000, 4000, 5000, 6000, 7000};
        int[] vmMips = {1000, 1000};
        long[] timeQuantums = {10, 20, 50};

        for (long quantum : timeQuantums) {
            System.out.println("\n" + "=".repeat(60));
            System.out.println("Time Quantum: " + quantum + " ms");
            System.out.println("=".repeat(60));

            simulateRoundRobin(cloudletLengths, vmMips, quantum);
        }
    }

    private static void simulateRoundRobin(long[] cloudletLengths, int[] vmMips, long timeQuantum) {
        int numCloudlets = cloudletLengths.length;
        int numVMs = vmMips.length;

        Queue<Integer> readyQueue = new LinkedList<>();

        long[] remainingLength = new long[numCloudlets];
        boolean[] completed = new boolean[numCloudlets];
        double[] vmCurrentTime = new double[numVMs];
        double[] cloudletStartTime = new double[numCloudlets];
        double[] cloudletFinishTime = new double[numCloudlets];

        Arrays.fill(cloudletStartTime, -1);
        Arrays.fill(cloudletFinishTime, -1);

        for (int i = 0; i < numCloudlets; i++) {
            remainingLength[i] = cloudletLengths[i];
            readyQueue.add(i);
        }

        int totalCompleted = 0;
        int contextSwitches = 0;
        List<ExecutionStep> executionTrace = new ArrayList<>();

        while (totalCompleted < numCloudlets) {
            if (readyQueue.isEmpty()) break;

            int cloudletId = readyQueue.poll();
            if (completed[cloudletId]) continue;

            int selectedVM = 0;
            for (int i = 1; i < numVMs; i++) {
                if (vmCurrentTime[i] < vmCurrentTime[selectedVM]) {
                    selectedVM = i;
                }
            }

            if (cloudletStartTime[cloudletId] == -1) {
                cloudletStartTime[cloudletId] = vmCurrentTime[selectedVM];
            }

            long executeLength = Math.min(timeQuantum, remainingLength[cloudletId]);
            double startTime = vmCurrentTime[selectedVM];
            double executeTime = (double)executeLength / vmMips[selectedVM];
            double endTime = startTime + executeTime;

            executionTrace.add(new ExecutionStep(cloudletId, selectedVM, startTime, endTime, executeLength));

            remainingLength[cloudletId] -= executeLength;
            vmCurrentTime[selectedVM] = endTime;

            if (remainingLength[cloudletId] == 0) {
                completed[cloudletId] = true;
                cloudletFinishTime[cloudletId] = endTime;
                totalCompleted++;
            } else {
                readyQueue.add(cloudletId);
                contextSwitches++;
            }
        }

        System.out.println("Context Switches: " + contextSwitches);
        printExecutionOrderTable(executionTrace);
        printPerformanceMetrics(cloudletLengths, cloudletStartTime, cloudletFinishTime, vmMips);
        analyzeTimeQuantumEffect(timeQuantum, contextSwitches);
    }

    private static void printExecutionOrderTable(List<ExecutionStep> executionTrace) {
        System.out.println("\nExecution Order Table:");
        System.out.println("Step\tCloudlet\tVM\tStart Time\tEnd Time\tExecuted Length");
        System.out.println("------------------------------------------------------------------");

        DecimalFormat df = new DecimalFormat("0.00");

        for (int i = 0; i < Math.min(20, executionTrace.size()); i++) {
            ExecutionStep step = executionTrace.get(i);
            System.out.printf("%d\t%d\t\t%d\t%s\t\t%s\t\t%d\n",
                    i+1, step.cloudletId, step.vmId,
                    df.format(step.startTime), df.format(step.endTime), step.executedLength);
        }

        if (executionTrace.size() > 20) {
            System.out.println("... (showing first 20 steps out of " + executionTrace.size() + " total steps)");
        }
    }

    private static void printPerformanceMetrics(long[] cloudletLengths, double[] startTimes,
                                                double[] finishTimes, int[] vmMips) {
        System.out.println("\nPerformance Results:");
        System.out.println("Cloudlet\tTurnaround Time\t\tWaiting Time");
        System.out.println("------------------------------------------------");

        double totalTurnaround = 0;
        double totalWaiting = 0;
        DecimalFormat df = new DecimalFormat("0.00");

        for (int i = 0; i < cloudletLengths.length; i++) {
            double turnaround = finishTimes[i] - 0;
            double executionTime = (double)cloudletLengths[i] / vmMips[i % vmMips.length];
            double waiting = turnaround - executionTime;

            totalTurnaround += turnaround;
            totalWaiting += waiting;

            System.out.printf("%d\t\t%s\t\t\t%s\n",
                    i, df.format(turnaround), df.format(waiting));
        }

        System.out.println("------------------------------------------------");
        System.out.printf("Average Turnaround Time: %s seconds\n",
                df.format(totalTurnaround / cloudletLengths.length));
        System.out.printf("Average Waiting Time: %s seconds\n",
                df.format(totalWaiting / cloudletLengths.length));
    }

    private static void analyzeTimeQuantumEffect(long timeQuantum, int contextSwitches) {
        System.out.println("\nTime Quantum Analysis:");
        System.out.println("- Total Context Switches: " + contextSwitches);
        if (timeQuantum == 10) {
            System.out.println("- Small quantum: More context switches, better response time");
        } else if (timeQuantum == 20) {
            System.out.println("- Medium quantum: Balanced approach");
        } else if (timeQuantum == 50) {
            System.out.println("- Large quantum: Fewer switches, approaches FCFS");
        }
    }
}
