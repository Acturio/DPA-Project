import luigi
import subprocess

class ExperimentRTask(luigi.Task):

  date = luigi.DateParameter(default = None)
  exercise = luigi.Parameter(default="TRUE")
  limit = luigi.Parameter(default = "300000")
  initial = luigi.Parameter(default="T")

  def output(self):
    local_path = "sandbox/test_r2.csv"
    return luigi.local_target.LocalTarget(local_path, format = luigi.format.Nop)

  def run(self):
    par1 = self.date.strftime("%Y-%m-%d")
    par2 = self.exercise
    par3 = self.limit
    par4 = self.initial
    #subprocess.call('Rscript src/pipeline/test_r.R', [par1, par2], shell=True)
    subprocess.call(['Rscript', 'src/pipeline/test_r.R', par1, par2, par3, par4])


# PYTHONPATH='.' luigi --module src.pipeline.LuigiExperimentRTask ExperimentRTask --date '2021-05-25' --exercise "hola" --limit "50" --initial "T" --local-scheduler