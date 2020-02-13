@Library('k8s-jenkins-pipeline')

import no.ok.build.k8s.jenkins.pipeline.stages.*
import no.ok.build.k8s.jenkins.pipeline.stages.python.*
import no.ok.build.k8s.jenkins.pipeline.stages.versionBumpers.*
import no.ok.build.k8s.jenkins.pipeline.pipeline.*
import no.ok.build.k8s.jenkins.pipeline.common.*
import java.util.function.Predicate

String test = """
              make test BUILD_VENV=/tmp/virtualenv
              """
String buildAndBumpVersion = """
                make clean build jenkins-bump-patch BUILD_VENV=/tmp/virtualenv
                """

String deployDev = """
                npm install
                serverless deploy --stage dev
                """

PythonConfiguration.instance.setContainerRepository("container-registry.oslo.kommune.no/python-37-serverless")
PythonConfiguration.instance.setPythonVersion("0.2.2")

PythonConfiguration.instance.addSecretEnvVar("NEXUS_PASSWORD", "nexus-credentials", "password")
PythonConfiguration.instance.addSecretEnvVar("NEXUS_USERNAME", "nexus-credentials", "username")

PythonConfiguration.instance.addSecretEnvVar("AWS_ACCESS_KEY_ID", "aws-jenkins-credentials", "AWS_ACCESS_KEY_ID")
PythonConfiguration.instance.addSecretEnvVar("AWS_SECRET_ACCESS_KEY", "aws-jenkins-credentials", "AWS_SECRET_ACCESS_KEY")

Pipeline pipeline = new Pipeline(this)
  .addStage(new ScmCheckoutStage())
  .abortIf(new LastCommitWasVersionBump(this))
  .addStage(new PythonStage(test))

if(env.BRANCH_NAME == "master"){
  pipeline
  //.addStage(new PythonStageWithGit(buildAndBumpVersion, "Bump Version"))
  //.addStage(new PythonStage(deployDev)) # Uncomment for automatic deployment

}

pipeline.execute()

class PythonStageWithGit extends PythonStage {
    private String script
    final String stageName
    private String credentialsId

    PythonStageWithGit(String script, String stageName = "Python with git 🗂") {
        super(script)
        this.script = script
        this.stageName = stageName
    }

    @Override
    void execute(def src) {
        PythonConfiguration config = PythonConfiguration.instance
        src.stage(this.stageName) {
            src.container(config.CONTAINER_NAME) {
                this.credentialsId = src.env.GITHUB_CREDENTIALS_ID
                src.withCredentials([[$class: 'UsernamePasswordMultiBinding', credentialsId: this.credentialsId, usernameVariable: 'GIT_USERNAME', passwordVariable: 'GIT_PASSWORD']]) {

                    src.sh "git config user.email ${src.env.GITHUB_EMAIL}"
                    src.sh "git config user.name ${src.env.GIT_USERNAME}"
                    src.sh "git config credential.username ${src.env.GIT_USERNAME}"
                    src.sh "git config credential.helper '!echo password=\$GIT_PASSWORD; echo'"
                    src.sh "python -m venv /tmp/virtualenv"
                    src.sh """#!/bin/bash
                       source /tmp/virtualenv/bin/activate
                       ${this.script}
                       """
                }
            }
        }
    }
}

class LastCommitWasVersionBump implements Predicate<Stage> {
    private final def src

    LastCommitWasVersionBump(def jenkinsfile) {
        src = jenkinsfile
    }

    @Override
    boolean test(final Stage stage) {
        String lastCommit = src.sh(returnStdout: true, script: 'git log -1 --pretty=%B')
        lastCommit.startsWith("Bump version: ")
    }

}