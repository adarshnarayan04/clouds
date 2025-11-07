package org.cloudbus.cloudsim.examples;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.*;
import java.text.DecimalFormat;
import java.util.*;

public class rr_perf {

    static class CloudletRR {
        int id;
        double arrival;
        long burst;
        long remaining;
        Cloudlet cloudlet;

        CloudletRR(int id, double arrival, long burst, Cloudlet c) {
            this.id = id;
            this.arrival = arrival;
            this.burst = burst;
            this.remaining = burst;
            this.cloudlet = c;
        }
    }

    public static void main(String[] args) {
        try {
            Scanner sc = new Scanner(System.in);

            System.out.println("-------- ROUND ROBIN CLOUDSIM (With Arrival Time) --------");

            System.out.print("Enter number of Cloudlets: ");
            int n = sc.nextInt();

            double[] arrival = new double[n];
            long[] burst = new long[n];

            for (int i = 0; i < n; i++) {
                System.out.print("Enter arrival time of Cloudlet " + i + ": ");
                arrival[i] = sc.nextDouble();

                System.out.print("Enter burst time (MI) of Cloudlet " + i + ": ");
                burst[i] = sc.nextLong();
            }

            System.out.print("Enter number of VMs: ");
            int vmCount = sc.nextInt();

            System.out.print("Enter MIPS per VM: ");
            int mips = sc.nextInt();

            double TIME_QUANTUM = 10.0;  // ms

            CloudSim.init(1, Calendar.getInstance(), false);

            Datacenter dc = createDatacenter("DC_1");

            DatacenterBroker broker = new DatacenterBroker("Broker1");

            List<Vm> vmlist = new ArrayList<>();
            int brokerId = broker.getId();

            // Create VMs
            for (int i = 0; i < vmCount; i++) {
                Vm vm = new Vm(i, brokerId, mips, 1, 512, 1000, 10000,
                        "Xen", new CloudletSchedulerTimeShared());
                vmlist.add(vm);
            }
            broker.submitGuestList(vmlist);

            // Create Cloudlets and RR Structs
            List<Cloudlet> cloudletList = new ArrayList<>();
            List<CloudletRR> rrJobs = new ArrayList<>();

            UtilizationModel um = new UtilizationModelFull();

            for (int i = 0; i < n; i++) {
                Cloudlet c = new Cloudlet(i, burst[i], 1, 300, 300, um, um, um);
                c.setUserId(brokerId);
                c.setVmId(i % vmCount);

                cloudletList.add(c);
                rrJobs.add(new CloudletRR(i, arrival[i], burst[i], c));
            }

            broker.submitCloudletList(cloudletList);

            // Sort jobs by arrival time for RR queue simulation
            rrJobs.sort(Comparator.comparingDouble(j -> j.arrival));

            System.out.println("\n======= ROUND ROBIN EXECUTION =======");
            System.out.printf("%6s %12s %12s\n", "CL#", "Start", "Finish");

            Queue<CloudletRR> queue = new LinkedList<>();
            double currentTime = 0;
            int index = 0;
            int contextSwitch = 0;

            Map<Integer, Double> firstStart = new HashMap<>();
            Map<Integer, Double> finishTime = new HashMap<>();

            while (!queue.isEmpty() || index < rrJobs.size()) {

                // Add jobs that have arrived by current time
                while (index < rrJobs.size() && rrJobs.get(index).arrival <= currentTime) {
                    queue.add(rrJobs.get(index));
                    index++;
                }

                // If no job has arrived, jump to next arrival time
                if (queue.isEmpty()) {
                    currentTime = rrJobs.get(index).arrival;
                    continue;
                }

                CloudletRR job = queue.poll();

                if (!firstStart.containsKey(job.id)) {
                    firstStart.put(job.id, currentTime);
                }

                // CPU executes MI based on MIPS (MI/ms = mips/1000)
                double exec = Math.min(TIME_QUANTUM, job.remaining / (mips / 1000.0));
                double start = currentTime;
                currentTime += exec;
                job.remaining -= exec * (mips / 1000.0);
                double end = currentTime;

                System.out.printf("%6d %12.2f %12.2f\n", job.id, start, end);

                if (job.remaining > 0) {
                    queue.add(job); // Put back if unfinished
                } else {
                    finishTime.put(job.id, end); // Completed
                }

                contextSwitch++;
            }

            System.out.println("\nContext Switches: " + contextSwitch);

            // Calculate Waiting + Turnaround
            System.out.println("\n------- FINAL RESULTS -------");
            System.out.printf("%6s %12s %12s %12s\n", "CL#", "Arrival", "Waiting", "Turnaround");

            double totalWait = 0, totalTAT = 0;

            for (CloudletRR j : rrJobs) {
                double tat = finishTime.get(j.id) - j.arrival;
                double execTime = j.burst / (mips / 1000.0);
                double wait = tat - execTime;

                totalWait += wait;
                totalTAT += tat;

                System.out.printf("%6d %12.2f %12.2f %12.2f\n", j.id, j.arrival, wait, tat);
            }

            System.out.println("\nAverage Waiting Time: " + (totalWait / n));
            System.out.println("Average Turnaround Time: " + (totalTAT / n));

            CloudSim.startSimulation();
            CloudSim.stopSimulation();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Datacenter createDatacenter(String name) {

        List<Pe> peList = new ArrayList<>();
        peList.add(new Pe(0, new PeProvisionerSimple(1000)));

        List<Host> hostList = new ArrayList<>();
        hostList.add(new Host(0,
                new RamProvisionerSimple(4096),
                new BwProvisionerSimple(10000),
                1000000,
                peList,
                new VmSchedulerTimeShared(peList))
        );

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                "x86", "Linux", "Xen", hostList, 10, 3.0, 0.05, 0.001, 0);

        Datacenter dc = null;
        try {
            dc = new Datacenter(name, characteristics,
                    new VmAllocationPolicySimple(hostList), new LinkedList<>(), 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dc;
    }
}