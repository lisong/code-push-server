import { DataTypes, Model } from 'sequelize';
import { sequelize } from '../core/utils/connections';

export interface DeploymentsVersionsInterface extends Model {
    id: number;
    deployment_id: number;
    app_version: string;
    current_package_id: number;
    min_version: number;
    max_version: number;
    created_at: Date;
    updated_at: Date;
}

export const DeploymentsVersions = sequelize.define<DeploymentsVersionsInterface>(
    'DeploymentsVersions',
    {
        id: {
            type: DataTypes.INTEGER({ length: 10 }),
            allowNull: false,
            autoIncrement: true,
            primaryKey: true,
        },
        deployment_id: DataTypes.INTEGER({ length: 10 }),
        app_version: DataTypes.STRING,
        current_package_id: DataTypes.INTEGER({ length: 10 }),
        min_version: DataTypes.BIGINT({ length: 20 }),
        max_version: DataTypes.BIGINT({ length: 20 }),
        created_at: DataTypes.DATE,
        updated_at: DataTypes.DATE,
    },
    {
        tableName: 'deployments_versions',
        underscored: true,
        paranoid: true,
    },
);
