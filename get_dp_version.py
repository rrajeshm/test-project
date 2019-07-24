import subprocess
import sys
import argparse
import os
# kubectl describe deployments | grep Image


class dp_version():
    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-l', required=True, dest='lab_name', help="Provide labname")
        args = parser.parse_args()
        self.lab_name = args.lab_name

        print "Lab :", self.lab_name

        if not self._get_kubeconfig():
            return False
        if not self.select_project():
            return False

    def _get_kubeconfig(self):
        dir_path = os.path.dirname(os.path.abspath(__file__))
        print dir_path
        if os.path.exists(os.path.join(dir_path, "dp_labs")):
            kube_config = os.path.join(dir_path, "dp_labs", self.lab_name, "vmr", "kubeconfig")
        else:
            kube_config = os.path.join(dir_path, "conf", self.lab_name, "vmr", "kubeconfig")
        print "KubeConfig :", kube_config
        if os.path.exists(kube_config):
            os.environ['KUBECONFIG'] = kube_config
            return True
        else:
            print "Kubecofig file not found"
            return False

    def select_project(self, project="vmr"):
        try:
            subprocess.check_output("oc project %s" % project, shell=True)
            return True
        except Exception as e:
            return False

    def get_vmr_version(self, vmr_deployment="vmr"):
        try:
            vmr_version = []
            if not self.select_project(vmr_deployment):
                msg = "Deployment %s not present " % vmr_deployment
                return False, msg

            cmd = "oc describe deployments | grep Image:"

            code, details = self.execute_cmd(cmd)
            if code != 0:
                # print details
                return False, details
            # print details
            for ln in details.split("\n"):
                if "ZK" not in ln:
                    vmr_version.append(ln.split(":")[-1])

            return True, list(set(vmr_version))

        except Exception as e:
            # print str(e)
            return False, str(e)

    def get_mpe_version(self, mpe_deployment="mpe-standalone"):
        try:
            mpe_version = {}
            if self.select_project(mpe_deployment):
                pass
            elif self.select_project("playoutbundle"):
                pass
            else:
                msg = "Project mpe-standalone / playoutbundle not present"
                return False, msg

            cmd = "oc describe deployments | grep Image:"

            code, details = self.execute_cmd(cmd)
            if code != 0:
                return False, details

            vmp_temp = []
            mpe_temp = []
            for ln in details.split("\n"):
                if "vmp" in ln and "mpe" not in ln:
                    image_det = ln.split(":")[1]
                    vmp_temp.append(image_det.split("/")[-2])
                if "mpe" in ln:
                    mpe_temp.append(ln.split(":")[-1])

            if vmp_temp:
                mpe_version["VMP"]=list(set(vmp_temp))[0]
            if mpe_temp:
                mpe_version["MPE"]=list(set(mpe_temp))[0]

            return True, mpe_version

        except Exception as e:
            return False, str(e)

    def execute_cmd(self, command):
        """
        Execute a command on the local machine.
        :param command: the command to execute
        :return: (process return/exit code, the output of the executed command)
        """

        process_return_code = 1  # Can be any non-zero value
        output = None

        try:
            process_handler = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            output, error = process_handler.communicate()  # error contains None, since stderr is also redirected to STDOUT
            process_return_code = process_handler.returncode

        except (ValueError, OSError, BaseException) as e:
            print "Unable to execute local command=%s, exception=%s" % (command, e)

        return process_return_code, output.strip()


if __name__ == "__main__":
    run = dp_version()

    vmr_res, vmr_ver = run.get_vmr_version()
    if not vmr_res:
        print "Unable to get the VMR version : ", vmr_ver

    vmr = ",".join(vmr_ver)

    mpe_res, mpe_ver = run.get_mpe_version()
    if not mpe_res:
        print "Unable to get the MPE version :", mpe_ver

    print "VMR : ", vmr
    for k,v in mpe_ver.items():
        print "%s : %s" %(k, v)

    with open("dp_versions.txt", "w") as fp:
        fp.write("VMR : %s\n"% vmr)
        for k, v in mpe_ver.items():
            fp.write("%s : %s" % (k, v))
            fp.write("\n")



