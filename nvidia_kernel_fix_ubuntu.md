---
# Fixing NVIDIA/CUDA Breakage After Kernel Update on Ubuntu 22.04 (Dual RTX 3090)

## Problem
After an automatic kernel update (from `6.11.0-24` → `6.11.0-26`), `nvidia-smi` fails:

```bash
NVIDIA-SMI has failed because it couldn't communicate with the NVIDIA driver.
```

Reason: NVIDIA kernel modules are kernel-specific. When the kernel updated, the modules were missing for the new kernel.

---

## Step 1: Identify Current Kernel and Installed Kernels
```bash
uname -r               # Shows current running kernel
dpkg --list | grep linux-image  # Lists all installed kernels
```
Example output:
```
ii  linux-image-6.11.0-24-generic
ii  linux-image-6.11.0-26-generic
it  linux-image-6.14.0-29-generic
```

- `6.11.0-24` → last working kernel  
- `6.11.0-26` → broken NVIDIA driver  
- `6.14.0-29` → newer kernel pulled by auto-update

---

## Step 2: Rebuild / Reinstall NVIDIA Driver for the Working Kernel

1. **Purge existing drivers to avoid conflicts:**
```bash
sudo apt-get purge 'nvidia*'
sudo apt-get autoremove
```

2. **Install recommended driver (580 series):**
```bash
sudo apt update
sudo apt install nvidia-driver-580-open
```

3. **Reboot and verify:**
```bash
sudo reboot
nvidia-smi
```
Output should show both RTX 3090 GPUs and driver version `580.82.09`.

---

## Step 3: Prevent This from Happening Again

### 3.1 Hold the working kernel
```bash
sudo apt-mark hold linux-image-6.11.0-24-generic
sudo apt-mark hold linux-headers-6.11.0-24-generic
```

### 3.2 Hold the NVIDIA driver
```bash
sudo apt-mark hold nvidia-driver-580-open
```

### 3.3 Remove newer kernels (optional)
```bash
sudo apt remove --purge linux-image-6.11.0-26-generic linux-headers-6.11.0-26-generic
sudo apt remove --purge linux-image-6.14.0-29-generic linux-headers-6.14.0-29-generic
sudo update-grub
```

### 3.4 Prevent automatic HWE kernel upgrades
```bash
sudo apt-mark hold linux-generic-hwe-24.04
sudo apt-mark hold linux-image-generic-hwe-24.04
sudo apt-mark hold linux-headers-generic-hwe-24.04
```
> Ensures Ubuntu won’t automatically install a newer HWE kernel that breaks NVIDIA/CUDA.

---

## Step 4: Optional Verification
```bash
dpkg --list | grep linux-image   # Check installed kernels
apt-mark showhold                # Check held packages
nvidia-smi                       # Verify GPUs are recognized
```

---

## Summary / Lessons Learned
1. Kernel updates can break NVIDIA drivers because modules are kernel-specific.  
2. DKMS rebuild only works if the driver is registered with DKMS; otherwise reinstall the driver.  
3. Hold working kernel and NVIDIA driver to prevent auto-updates from breaking CUDA.  
4. Remove newer kernels to avoid accidental boot into a broken kernel.  
5. Hold HWE meta-packages to stop Ubuntu from auto-installing newer kernels.

---

## Optional: Ready-to-Run Script
You can combine all steps into a single bash script (see next file or section) to automate fixing and locking the setup.