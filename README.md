## wip

create a temporary nginx http server within namespace with udn  
use temporary server as source for datavolume

```bash
❯ oc get dv -A
NAMESPACE                            NAME                           PHASE       PROGRESS   RESTARTS   AGE
openshift-virtualization-os-images   centos-stream10-1d7e2727e6e4   Succeeded   100.0%                41h
openshift-virtualization-os-images   centos-stream10-840d19d49be8   Succeeded   100.0%                2d14h
openshift-virtualization-os-images   centos-stream9-05a83de68e2f    Succeeded   100.0%                2d14h
openshift-virtualization-os-images   fedora-68ed96832eca            Succeeded   100.0%                2d14h
openshift-virtualization-os-images   rhel10-c03936a065f2            Succeeded   100.0%                2d14h
openshift-virtualization-os-images   rhel8-4ccd8b6aee47             Succeeded   100.0%                2d14h
openshift-virtualization-os-images   rhel9-ab4ec16077fe             Succeeded   100.0%                2d14h

❯ go mod tidy

❯ go build -o udn-image-uploader ./cmd

❯ ./udn-image-uploader --namespace 9831783a-citrixudn --name golden-image --size 30Gi --storage-class gp3-csi --image-path ./Fedora-Cloud-Base-Generic-43-1.6.x86_64.qcow2
No Primary UDN detected in namespace 9831783a-citrixudn, using standard upload flow
Error uploading image: standard upload flow not implemented - use virtctl image-upload for non-UDN namespaces

❯ # another workflow can be used when udn is not detected, this code just exits

❯ ./udn-image-uploader --namespace green-namespace --name golden-image --size 30Gi --storage-class gp3-csi --image-path ./Fedora-Cloud-Base-Generic-43-1.6.x86_64.qcow2
Detected Primary UDN in namespace green-namespace, using HTTP source workflow
Creating ephemeral image server pod...
Creating image server service...
Streaming image ./Fedora-Cloud-Base-Generic-43-1.6.x86_64.qcow2 to pod...
Image size: 583335936 bytes (0.54 GB)
Wrote 583335936 bytes to tar stream
Creating DataVolume with HTTP source...
Waiting for DataVolume to complete...
DataVolume phase: ImportScheduled
DataVolume phase: ImportInProgress
DataVolume phase: ImportScheduled
DataVolume phase: ImportInProgress
DataVolume phase: Succeeded
Golden image golden-image created successfully
Cleaning up ephemeral resources...
Upload completed successfully!

❯ oc get dv -A
NAMESPACE                            NAME                           PHASE       PROGRESS   RESTARTS   AGE
green-namespace                      golden-image                   Succeeded   100.0%                2m13s
openshift-virtualization-os-images   centos-stream10-1d7e2727e6e4   Succeeded   100.0%                41h
openshift-virtualization-os-images   centos-stream10-840d19d49be8   Succeeded   100.0%                2d14h
openshift-virtualization-os-images   centos-stream9-05a83de68e2f    Succeeded   100.0%                2d14h
openshift-virtualization-os-images   fedora-68ed96832eca            Succeeded   100.0%                2d14h
openshift-virtualization-os-images   rhel10-c03936a065f2            Succeeded   100.0%                2d14h
openshift-virtualization-os-images   rhel8-4ccd8b6aee47             Succeeded   100.0%                2d14h
openshift-virtualization-os-images   rhel9-ab4ec16077fe             Succeeded   100.0%                2d14h
```
