package com.microsoft.azure.maven.spark.ux

import rx.Scheduler
import rx.schedulers.Schedulers

object MavenScheduler: IdeSchedulers {
    override fun processBarVisibleAsync(title: String?): Scheduler {
        return Schedulers.trampoline()
    }

    override fun processBarVisibleSync(title: String?): Scheduler {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun dispatchUIThread(): Scheduler {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}